ORDER_CREATED_TOPIC = "order_details"
ORDER_CONFIRMED_TOPIC = "order_confirmed"

# Use the machine IPv4 address while running as a docker container
BOOTSTRAP_SERVER = "192.168.10.7:29092"

# use this while running as standalone app
# BOOTSTRAP_SERVER = "localhost:29092"


ORDER_LIMIT = 100  # Produce maximum these many orders
SLEEP_FOR = 5  # Produce orders after every specified seconds
CONSUMER_POLLING_INTERVAL = 1  # polls for incoming messages
