import os
import logging
import signal
import threading
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

NEXT_ID = (ID + 1) % SUM_AMOUNT

def _get_aggregator_index(fruit):
    return int(hashlib.md5(fruit.encode()).hexdigest(), 16) % AGGREGATION_AMOUNT


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.token_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{ID}"]
        )

        self.next_token_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{NEXT_ID}"]
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_client = {}
        self.chunks_processed = {}
        self.contributed = {}

        self.already_sent_for_client = set()

        self.lock = threading.Lock()

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("SIGTERM received, stopping consumers")
        self.input_queue.stop_consuming()
        self.token_exchange.stop_consuming()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data from client {client_id}, fruit {fruit}, amount {amount}")
        fruits_of_client = self.amount_by_client.setdefault(client_id, {})
        fruits_of_client[fruit] = fruits_of_client.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

        with self.lock:
            self.chunks_processed[client_id] = self.chunks_processed.get(client_id, 0) + 1

    def _flush(self, client_id):
        logging.info(f"Flushing partial result for client {client_id}")
        fruits_of_client = self.amount_by_client.pop(client_id, {})

        for final_fruit_item in fruits_of_client.values():
            index = _get_aggregator_index(final_fruit_item.fruit)
            self.data_output_exchanges[index].send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

        with self.lock:
            self.already_sent_for_client.add(client_id)

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._send_token(*fields)
        ack()

    def _send_token(self, client_id, total_chunks):
        with self.lock:
            my_count = self.chunks_processed.get(client_id, 0)
            self.contributed[client_id] = my_count

        logging.info(f"Sending token for client {client_id}, my_count={my_count}, total_chunks={total_chunks}")
        self.next_token_exchange.send(
            message_protocol.internal.serialize([client_id, my_count, total_chunks])
        )

    def _process_token(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id, accumulated_count, total_chunks = fields
        logging.info(f"Received token for client {client_id}, accumulated={accumulated_count}, total={total_chunks}")

        with self.lock:
            already_sent = client_id in self.already_sent_for_client

        if already_sent:
            logging.info(f"Token for client {client_id} completed full ring, discarding")
            ack()
            return

        if accumulated_count == total_chunks:
            logging.info(f"Token match for client {client_id}, flushing")
            self._flush(client_id)
        else:
            with self.lock:
                my_count = self.chunks_processed.get(client_id, 0)
                prev_contributed = self.contributed.get(client_id, 0)
                self.contributed[client_id] = my_count

            accumulated_count = accumulated_count - prev_contributed + my_count

            logging.info(f"Token for client {client_id}: accumulated={accumulated_count}, total={total_chunks}")

        self.next_token_exchange.send(
            message_protocol.internal.serialize([client_id, accumulated_count, total_chunks])
        )
        ack()

    def start(self):
        token_thread = self._start_token_ring_thread()
        self.input_queue.start_consuming(self.process_data_messsage)
        token_thread.join()
        self.input_queue.close()
        self.token_exchange.close()
        for exchange in self.data_output_exchanges:
            exchange.close()

    def _start_token_ring_thread(self):
        token_thread = threading.Thread(
            target=self.token_exchange.start_consuming,
            args=(self._process_token,),
            daemon=True
        )
        token_thread.start()
        return token_thread


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
