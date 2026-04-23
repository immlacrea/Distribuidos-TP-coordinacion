import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}

        self.eof_count_by_client = {}

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("SIGTERM received, stopping consumer")
        self.input_exchange.stop_consuming()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Processing data message of client {client_id} fruit {fruit} amount {amount}")
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])

        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated_amount = fruit_top[i] + fruit_item.FruitItem(
                    fruit, amount
                )
                fruit_top.pop(i)
                bisect.insort(fruit_top, updated_amount)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        count = self.eof_count_by_client.get(client_id, 0) + 1
        self.eof_count_by_client[client_id] = count

        if count < SUM_AMOUNT:
            logging.info(f"EOF {count}/{SUM_AMOUNT} for client {client_id}, waiting")
            return

        logging.info(f"All EOFs received for client {client_id}, flushing")
        self.eof_count_by_client.pop(client_id)

        fruit_top_of_client = self.fruit_top_by_client.pop(client_id, [])

        fruit_chunk = list(fruit_top_of_client[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        if fruit_top:
            self.output_queue.send(message_protocol.internal.serialize([client_id] + fruit_top))
        self.output_queue.send(message_protocol.internal.serialize([client_id]))
        del fruit_top_of_client

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)
        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
