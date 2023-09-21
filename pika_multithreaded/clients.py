import functools
import logging
import pika
from pika.exceptions import (
    ProbableAuthenticationError,
    AuthenticationError,
    ProbableAccessDeniedError,
    AMQPConnectionError,
    ChannelWrongStateError)
import signal
import ssl
import sys
import threading
import time
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
        self._reconnect_delay = 0
        self.logger = logging.getLogger(__name__)

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

    @property
    def _is_connection_alive(self):
        return self.connection and self.connection.is_open

    def _clear_connection(self):
        self.connection = None
        self.channel = None

    def setup_signal_handlers(self):
        self.logger.debug("Setting up SIGINT/SIGTERM handler...")
        # Set up signal handlers since this client is intended to be run as its own process
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

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

    def connect(self, retry_if_failed=True):
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
        connection_attempt = 1
        while True:
            try:
                self.connection = pika.BlockingConnection(url_parameters)
                self.logger.debug("Connected successfully! Creating channel...")
                self.channel = self.connection.channel()
                self.logger.debug("Channel created successfully!")
                self._reconnect_delay = 0
                break
            except (ProbableAuthenticationError,
                    AuthenticationError,
                    ProbableAccessDeniedError) as ex:
                # If we have a credentials issue, we're never going to be able to connect so let's
                # just stop now
                self.logger.error(
                    f"ERROR: Incorrect connection credentials: {ex}")
                raise
            except Exception as ex:
                self.logger.error(
                    f"Unable to connect: {ex}",
                    exc_info=True)
                if not retry_if_failed:
                    break
            # Add some backoff seconds if we're supposed to retry the connection
            self._set_reconnect_delay()
            self.logger.info(
                f"Waiting {self._reconnect_delay} seconds before retrying the connection...")
            time.sleep(self._reconnect_delay)
            connection_attempt += 1
            self.logger.info(
                f"Attempting to connect again (attempt #{connection_attempt})...")

    def _reconnect_channel(self):
        if self._is_connection_alive:
            try:
                self.channel.close()
            except Exception:
                pass
            self.channel = self.connection.channel()
        else:
            try:
                self.close()
            except Exception:
                pass
            self.connect()

    def _set_reconnect_delay(self):
        """
        This function implements a growing reconnect delay. This allows the
        application to immediately reconnect if something happens to the AMQP
        connection. If the connection continually fails, however, then this
        gradually increases the delay time in order to patiently wait for the
        connection to be successful and not waste resources continually trying
        to reconnect.
        """
        self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30

    def close(self):
        # Stop consuming if we've started consuming already
        if self.consumer_tag:
            self.stop_consuming()
        # Close the connection
        if self._is_connection_alive:
            self.connection.close()
            self._clear_connection()
        elif self.connection:
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

    def ack_message(self, delivery_tag):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            self.connect()
            auto_close_connection = True
        # Publish a message to the correct location
        self.channel.basic_ack(delivery_tag=delivery_tag)
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()

    def ack_message_threadsafe(self, delivery_tag):
        if self.connection:
            self.connection.add_callback_threadsafe(
                functools.partial(self.ack_message, delivery_tag)
            )

    def nack_message(self, delivery_tag, requeue=True):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            self.connect()
            auto_close_connection = True
        # Publish a message to the correct location
        self.channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()

    def nack_message_threadsafe(self, delivery_tag, requeue=True):
        if self.connection:
            self.connection.add_callback_threadsafe(
                functools.partial(self.nack_message, delivery_tag, requeue)
            )

    def consume(
            self, queue, callback_function,
            auto_ack=False, consumer_tag=None, declare_queue=True, qos_count=1):
        # Keep track whether or not we need to auto-close the connection after we're done
        auto_close_connection = False
        if not self.connection:
            auto_close_connection = True
        # =============================
        keep_consuming = True
        self.user_consumer_callback = callback_function
        while keep_consuming:
            try:
                if not self.channel or self.channel.is_closed:
                    # This may occur when we're attempting to reconnect after a connection issue
                    self.logger.info(f"{'*'*20}\nConnecting...\n{'*'*20}")
                    self.connect()
                # Queue declare is idempotent so it doesn't matter if we call it many times back to
                # back. So if we're able to connect to the AMQP server, this guarantees there will
                # be a queue available to consume
                if declare_queue:
                    self.queue_declare(queue, durable=True)
                # Set QOS prefetch count. Now that this is multi-threaded, we can now control how
                # many messages we process in parallel by simply increasing this number.
                self.logger.debug(f"Setting QOS: {qos_count}...")
                self.channel.basic_qos(prefetch_count=qos_count)
                # Consume the queue
                if consumer_tag:
                    self.consumer_tag = consumer_tag
                else:
                    self.consumer_tag = f"pika-amqp-client-{str(uuid.uuid4())}"
                self.logger.debug(f"Waiting for messages on queue {queue}...")
                self.channel.basic_consume(
                    queue,
                    self._consumer_callback,
                    auto_ack=auto_ack,
                    consumer_tag=self.consumer_tag)
                self.channel.start_consuming()
                keep_consuming = False
            except AMQPConnectionError as ex:
                # There is a timeout of 1800000 ms that results in this exception so catch the
                # exception and re-start the consumer
                self.logger.error(
                    f"Connection Error: {ex}",
                    exc_info=True)
                keep_consuming = True
            except ChannelWrongStateError as ex:
                self.logger.error(
                    f"Channel Error: Possible usage of already closed channel --> {ex}",
                    exc_info=True)
                self.stop_consuming()
                self._reconnect_channel()
                keep_consuming = True
            except Exception as ex:
                self.logger.error(
                    f"FATAL ERROR: {ex}",
                    exc_info=True)
                self.logger.debug("General exception. Closing consumer...")
                self.stop_consuming()
                keep_consuming = False
        # =============================
        # Close the connection if we opened it at the beginning of this function
        if auto_close_connection:
            self.close()

    def _consumer_callback(self, channel, method, properties, body):
        new_thread = threading.Thread(
            target=self.user_consumer_callback,
            args=[self, channel, method, properties, body],
            daemon=True
        )
        new_thread.start()

    def _signal_handler(self, sig, frame):
        self.logger.warning(
            "*** AMQP Client terminating. Closing AMQP connection...")
        self.close()
        sys.exit(0)

    def stop_consuming(self):
        if self.consumer_tag and self._is_connection_alive and self.channel.is_open:
            self.channel.basic_cancel(self.consumer_tag)
            self.consumer_tag = None
            self.user_consumer_callback = None
