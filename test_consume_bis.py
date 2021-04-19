from PubSubLib.Consumer import Consumer
from PubSubLib.Enums import Topic, Key


@Consumer.subscribe(Topic.ESTIMA, "cdp_API")
def print_estima(message):
    print(message)
