import logging
from pika_multithreaded.clients import AmqpClient


def new_amqp_client():
    return AmqpClient(
        url=BROKER_URL)


def receive_message(amqp_client, _, method, properties, body):
    logger.debug("****** Message Received! ******")
    logger.debug(f"Channel: {amqp_client.channel}")
    logger.debug(f"Method: {method}")
    logger.debug(f"Properties: {properties}")
    logger.debug(f"Message: {body}")
    logger.debug("****** Message processed successfully! Acking... ******")
    amqp_client.ack_message_threadsafe(delivery_tag=method.delivery_tag)
    logger.debug("****** Message acked successfully! ******")


# Edit these lines to change the connection parameters
protocol = "amqps"
host = "[put-host-here]"
port = "5671"
user = "[put-user-here]"
password = "[put-password-here]"
# DON'T EDIT THE FOLLOWING LINES
BROKER_URL = f"{protocol}://{user}:{password}@{host}:{port}"
AMQP_QUEUE = "test-queue"
is_dry_run = False
logger = logging.getLogger(__name__)
# The root logger handler that we have set up in settings is only set for log level of
# "INFO" so if we want to override that, we need to add a new handler to this logger
# so that we can give it a different log level
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)
logger.debug("============== STANDALONE SCRIPT DEBUG ==============")
logger.debug("args: None cause I'm doing it myself")
# Connect to AMQP Broker
amqp_client = new_amqp_client()
# Set up signal handlers since this script is intended to be run as its own process
logger.debug("Setting up SIGINT/SIGTERM handler...")
amqp_client.setup_signal_handlers()
logger.debug(
    f"Establishing connection to AMQP Broker on queue '{AMQP_QUEUE}'")
with amqp_client as amqp_client:
    amqp_client.consume(
        AMQP_QUEUE,
        receive_message,
        auto_ack=False,
        declare_queue=True)
