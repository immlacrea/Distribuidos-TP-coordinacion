import os
import logging
import threading
import hashlib
import time

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


def _get_aggregator_index(fruit):
    return int(hashlib.md5(fruit.encode()).hexdigest(), 16) % AGGREGATION_AMOUNT


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.eof_control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{ID}"]
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.eof_broadcast_exchanges = []
        for i in range(SUM_AMOUNT):
            eof_broadcast_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{i}"]
                )
            self.eof_broadcast_exchanges.append(eof_broadcast_exchange)

        self.amount_by_client = {}

        self.processing_work_for_client = set()
        self.already_sent_for_client = set()
        self.eof_received = set()

        self.lock = threading.Lock()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data from client {client_id}, fruit {fruit}, amount {amount}")
        fruits_of_client = self.amount_by_client.setdefault(client_id, {})
        fruits_of_client[fruit] = fruits_of_client.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

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

        self.already_sent_for_client.add(client_id)

    def _process_data_and_track(self, ack, client_id, fruit, amount):
        with self.lock:
            self.processing_work_for_client.add(client_id)

        self._process_data(client_id, fruit, amount)

        with self.lock:
            self.processing_work_for_client.discard(client_id)

        ack()
        self._try_flush(client_id)

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data_and_track(ack, *fields)
        else:
            self._broadcast_eof(ack, *fields)

    def _broadcast_eof(self, ack, client_id):
        logging.info(f"Broadcasting EOF for client {client_id}")
        for exchange in self.eof_broadcast_exchanges:
            exchange.send(
                message_protocol.internal.serialize([client_id])
            )
        ack()

    def start(self):
        eof_thread = self._start_listening_eof_broadcast()
        self.input_queue.start_consuming(self.process_data_messsage)
        eof_thread.join()

    def _start_listening_eof_broadcast(self):
        eof_thread = threading.Thread(
            target=self.eof_control_exchange.start_consuming,
            args=(self._process_eof,),
            daemon=True
        )
        eof_thread.start()
        return eof_thread

    def _process_eof(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        logging.info(f"Received EOF broadcast for client {client_id}")
        with self.lock:
            self.eof_received.add(client_id)

        ack()
        self._try_flush(client_id)

    def _try_flush(self, client_id):
        with self.lock:
            if client_id not in self.eof_received:
                return
            if client_id in self.processing_work_for_client:
                return
            if client_id in self.already_sent_for_client:
                return
            self._flush(client_id)


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
