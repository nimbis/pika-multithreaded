import pika
import ssl
import uuid

from .utils import AmqpUtils


class AmqpClient:
    def __init__(self, host=None, port=None, user=None, password=None, use_ssl=False, url=None):
        if url:
            self.url = url
        else:
            self.url = AmqpUtils.generate_url(
                host, port, user, password, use_ssl)
        # Default the connection info to None to signal a connection has not been made yet
        self._clear_connection()
        self.consumer_tag = None

    def __enter__(self):
        """
        This allows the use of the "with AmqpClient:" syntax so that it will
        autoclose the connection when the block is done executing.
        """
        if not self.connection:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        This allows the use of the "with AmqpClient:" syntax so that it will
        autoclose the connection when the block is done executing.
        """
        self.close()

    def _clear_connection(self):
        self.connection = None
        self.channel = None

    def queue_declare(self, queue_name, durable=False):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            self.connect()
            auto_close_connection = True
        self.channel.queue_declare(queue_name, durable=durable)
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()

    def connect(self):
        if self.url.startswith("amqps://"):
            # Looks like we're making a secure connection
            # Create the SSL context for our secure connection. This context forces a more secure
            # TLSv1.2 connection and uses FIPS compliant cipher suites. To understand what suites
            # we're using here, read docs on OpenSSL cipher list format:
            # https://www.openssl.org/docs/man1.1.1/man1/ciphers.html
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')
            # Create the URL parameters for our connection
            url_parameters = pika.URLParameters(self.url)
            url_parameters.ssl_options = pika.SSLOptions(context=ssl_context)
        elif self.url.startswith("amqp://"):
            # Looks like we're making a clear-text connection
            # Create the connection and store them in self
            url_parameters = pika.URLParameters(self.url)
        else:
            raise Exception("AMQP URL must start with 'amqp://' or 'amqps://'")

        # Create the connection and store them in self
        self.connection = pika.BlockingConnection(url_parameters)
        self.channel = self.connection.channel()

    def close(self):
        # Stop consuming if we've started consuming already
        if self.consumer_tag:
            self.stop_consuming()
        # Close the connection
        if self.connection:
            self.connection.close()
            self._clear_connection()

    def send_message(self, routing_key, message, exchange=None):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            self.connect()
            auto_close_connection = True
        # Set the exchange to the default (empty string) if none was supplied
        if not exchange:
            exchange = ''
        # Publish a message to the correct location
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message
        )
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()

    def get_message(self, queue):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            self.connect()
            auto_close_connection = True
        # Attempt to get a message from the server
        method_frame, header_frame, body = self.channel.basic_get(queue)
        # If we get something back, ACK the message and return it. If not, return a bunch of Nuns.
        # (ha! get it?)
        if method_frame:
            self.channel.basic_ack(method_frame.delivery_tag)
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()
        return method_frame, header_frame, body

    def consume(self, queue, callback_function, auto_ack=False, consumer_tag=None):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            self.connect()
            auto_close_connection = True
        # Consume the queue
        self.consumer_tag = f"pika-amqp-client-{str(uuid.uuid4())}"
        self.channel.basic_consume(
            queue, callback_function, auto_ack=auto_ack, consumer_tag=self.consumer_tag)
        self.channel.start_consuming()
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()

    def stop_consuming(self):
        if self.consumer_tag:
            self.channel.basic_cancel(self.consumer_tag)
            self.consumer_tag = None
