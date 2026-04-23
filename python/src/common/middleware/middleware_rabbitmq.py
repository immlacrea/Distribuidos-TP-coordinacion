import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._queue = queue_name
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self._queue, durable=True, arguments={'x-queue-type': 'quorum'})

    def close(self):
        try:
            if self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))

    def start_consuming(self, on_message_callback):

        def callback(ch, method, properties, body):
            ack = lambda : ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda : ch.basic_nack(delivery_tag=method.delivery_tag)
            try:
                on_message_callback(body, ack, nack)
            except Exception as e:
                raise MessageMiddlewareMessageError(str(e))

        try:
            self._channel.basic_qos(prefetch_count=50)
            self._channel.basic_consume(
                queue=self._queue,
                on_message_callback=callback)
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))


    def stop_consuming(self):
        try:
            self._connection.add_callback_threadsafe(
                self._channel.stop_consuming
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))


    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange='',
                routing_key=self._queue,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                )
            )
            #self.close()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self._routing_keys = routing_keys
        self._exchange_name = exchange_name

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))
        self._channel = self._connection.channel()

        self._channel.exchange_declare(exchange=exchange_name, exchange_type='direct')


    def close(self):
        try:
            if self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))


    def start_consuming(self, on_message_callback):

        def callback(ch, method, properties, body):
            ack = lambda : ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda : ch.basic_nack(delivery_tag=method.delivery_tag)
            try:
                on_message_callback(body, ack, nack)
            except Exception as e:
                raise MessageMiddlewareMessageError(str(e))

        try:
            result = self._channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue

            for routing_key in self._routing_keys:
                self._channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=queue_name,
                    routing_key=routing_key)
            self._channel.basic_qos(prefetch_count=50)
            self._channel.basic_consume(queue=queue_name, on_message_callback=callback)
            self._channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))


    def stop_consuming(self):
        try:
            self._connection.add_callback_threadsafe(
                self._channel.stop_consuming
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))


    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=self._routing_keys[0],
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))