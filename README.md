# Kafka-To-Wörterbuch

kafka-to-worterbuch subscribes to your kafka broker and uses its messages to build an application state store in Wörterbuch. Individual applications are properly namespaced so they do not get in conflict with each other. Messages are processed based on an easy to understand set of rules, adding, updating or deleting state in Wörterbuch.

By committing message offsets to 