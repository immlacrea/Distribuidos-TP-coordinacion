import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.eof_count_by_client = {}
        self.fruit_top_by_client = {}

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        payload = fields[1:]

        if len(payload) == 0:
            count = self.eof_count_by_client.get(client_id, 0) + 1
            self.eof_count_by_client[client_id] = count

            if count < AGGREGATION_AMOUNT:
                logging.info(f"EOF {count}/{AGGREGATION_AMOUNT} for client {client_id}, waiting")
                ack()
                return

            logging.info(f"All EOFs received for client {client_id}, flushing")
            self.eof_count_by_client.pop(client_id)
            combined = self.fruit_top_by_client.pop(client_id, [])
            combined.sort(key=lambda x: x[1], reverse=True)
            top = combined[:TOP_SIZE]
            self.output_queue.send(
                message_protocol.internal.serialize([client_id] + top)
            )
        else:
            logging.info(f"Processing data message of client {client_id} data: {payload}")
            fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
            fruit_top.extend(payload)

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
