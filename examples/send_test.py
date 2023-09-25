from pika_multithreaded.clients import AmqpClient

# Edit these lines to change the connection parameters
protocol = "amqps"
host = "[put-host-here]"
port = "5671"
queue = "test-queue"
user = "[put-user-here]"
password = "[put-password-here]"
# DON'T EDIT THE FOLLOWING LINE
url = f"{protocol}://{user}:{password}@{host}:{port}"

print(">>> Creating connection object...")
my_connection = AmqpClient(url=url)
print(">>> Connection object created!")
print(">>> Sending message...")
my_connection.send_message(
    routing_key="test-queue",
    message="this is a test message. can you hear me???")

print(">>> Message sent successfully!!! (maybe)")
