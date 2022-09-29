# =================================
# EXAMPLE SINGLE THREADED CONSUMER
# =================================
#      Author: Ryan Stutzman
# Description: This is an example usage of a basic single threaded consumer
# =================================
import json
import logging
from pika.exceptions import ChannelClosedByBroker
import signal

from pika_multithreaded.clients import AmqpClient

logger = logging.getLogger(__name__)


class ExampleMultithreadedFunctions:
    @staticmethod
    def test_function_1(amqp_client, body, is_dry_run):
        logger.debug("Entering 'test_function_1' function...")
        # Create a new message to send somewhere else
        amqp_message_data = {
            "version": "0.0.1",
            "requeue_or_fail": "fail",
            "message_format": "test_function_2",
            "payload": {
                "my_key": "my_value",
                "previous_body": body
            }
        }
        ExampleMultithreadedFunctions.util_send_message(
            amqp_client,
            is_dry_run,
            "",
            "test-queue-2",
            json.dumps(amqp_message_data)
        )

    @staticmethod
    def test_function_2(amqp_client, body, is_dry_run):
        logger.debug("Entering 'test_function_2' function...")
        # Create a new message to send somewhere else
        amqp_message_data = {
            "version": "0.0.1",
            "requeue_or_fail": "fail",
            "message_format": "test_function_1",
            "payload": {
                "my_key": "my_value",
                "previous_body": body
            }
        }
        ExampleMultithreadedFunctions.util_send_message(
            amqp_client,
            is_dry_run,
            "",
            "test-queue-2",
            json.dumps(amqp_message_data)
        )

    @staticmethod
    def util_log_amqp_message(exchange, queue, message):
        log_msg = "******** AMQP Message ********\n"
        log_msg += f"Exchange: '{exchange}'\n"
        log_msg += f"   Queue: '{queue}'\n"
        log_msg += f" Message: '{message}'\n"
        log_msg += "******************************\n"
        logger.info(log_msg)

    @staticmethod
    def util_send_message(amqp_client, is_dry_run, to_exchange, to_routing_key, body):
        if not is_dry_run:
            logger.debug(
                f"Sending '{body}' to '{to_routing_key}' via '{to_exchange}'")
            amqp_client.channel.basic_publish(
                exchange=to_exchange,
                routing_key=to_routing_key,
                body=body
            )
        else:
            ExampleMultithreadedFunctions.util_log_amqp_message(
                to_exchange,
                to_routing_key,
                body)


class ExampleMultithreadedConsumer:
    def __init__(self, queue="test-queue", debug=False, dry_run=False, broker_url="amqp://"):
        self.is_dry_run = dry_run
        self.is_debug = debug
        self._amqp_queue = queue
        self._amqp_broker_url = broker_url

    def run(self):
        if self.is_debug:
            # The root logger handler that we have set up in settings is only set for log level of
            # "INFO" so if we want to override that, we need to add a new handler to this logger
            # so that we can give it a different log level
            logger.addHandler(logging.StreamHandler())
            logger.setLevel(logging.DEBUG)
        logger.debug("============== STANDALONE SCRIPT DEBUG ==============")
        logger.debug("Setting up SIGINT/SIGTERM handler...")
        # Set up signal handlers since this script is intended to be run as its own process
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        # Connect to AMQP Broker
        self.amqp_client = AmqpClient(url=self._amqp_broker_url)
        with self.amqp_client as amqp_client:
            amqp_client.queue_declare(self._amqp_queue, durable=True)
            keep_consuming = True
            while keep_consuming:
                logger.debug(f"Connecting to queue {self._amqp_queue}...")
                try:
                    amqp_client.consume(
                        self._amqp_queue,
                        self.receive_message,
                        auto_ack=False)
                    keep_consuming = False
                except KeyboardInterrupt:
                    # Keyboard interrupt for when a user hits Crtl+C
                    keep_consuming = False
                except ChannelClosedByBroker:
                    # There is a timeout of 1800000 ms that results in this exception so catch the
                    # exception and re-start the consumer
                    logger.debug(
                        "The consumer timed out (default is 1800000 ms). Restarting now...")
                    keep_consuming = True

    def signal_handler(self, sig, frame):
        logger.warning("*** Emulator message service terminating. Closing AMQP connection...")
        self.amqp_client.stop_consuming()

    def receive_message(self, _, method, properties, body):
        logger.debug("****** Message Received! ******")
        logger.debug(f"Channel: {self.amqp_client.channel}")
        logger.debug(f"Method: {method}")
        logger.debug(f"Properties: {properties}")
        logger.debug(f"Message: {body}")
        try:
            msg_json = json.loads(body)
        except Exception as ex:
            logger.error(f"ERROR: Unable to parse message as JSON\n{ex}")
            # Acknowledge the message to kill it since we can't even read it
            # It may be best to set up a dead letter queue in case we want to review bad/ignored
            # messages at a later date, but for now we just kill them
            self.amqp_client.channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            # We don't want to continue with processing this message if the message isn't in a
            # a format that we can actually process
            return

        try:
            logger.debug(f"Parsed JSON: {msg_json}")
            # Throw an error if the 'message_format' field is not provided
            if "message_format" not in msg_json:
                raise Exception("Unknown message format: 'message_format' not provided")
            # test_function_1
            if msg_json["message_format"] == "test_function_1":
                ExampleMultithreadedFunctions.test_function_1(
                    self.amqp_client, msg_json["payload"], self.is_dry_run)
            # test_function_2
            elif msg_json["message_format"] == "test_function_2":
                ExampleMultithreadedFunctions.test_function_2(
                    self.amqp_client, msg_json["payload"], self.is_dry_run)
            logger.debug("****** Message processed successfully!")
            # Acknowledge the message
            self.amqp_client.channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as ex:
            logger.error(f"ERROR: Unable to process message\n{ex}")
            # Acknowledge the message to kill it since we can't even read it
            # Respect the "requeue_or_fail" flag here if it's the original message. If the message
            # has already been re-queued, then kill the message after the second error
            should_requeue = "requeue_or_fail" in msg_json and msg_json[
                "requeue_or_fail"] == "requeue"
            # We only want to requeue this if it is the original message and not requeued
            should_requeue = should_requeue and not method.redelivered
            if should_requeue:
                self.amqp_client.channel.basic_nack(
                    delivery_tag=method.delivery_tag, requeue=True)
            else:
                self.amqp_client.channel.basic_nack(
                    delivery_tag=method.delivery_tag, requeue=False)


if "__name__" == "__main__":
    example_obj = ExampleMultithreadedConsumer(
        debug=True,
        dry_run=False
    )
    example_obj.run()
