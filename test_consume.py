from PubSubLib.Consumer import Consumer
from PubSubLib.Enums import Topic, Key

consumer = Consumer("estimaapi")


@consumer.subscribe(Topic.ESTIMA)
def print_estima(message):
    print(message)

consumer.consume()
