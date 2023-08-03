# Kafka-To-Wörterbuch

kafka-to-worterbuch subscribes to your kafka broker and uses its messages to build an application state store in Wörterbuch. Individual applications are properly namespaced so they do not get in conflict with each other. Messages are processed based on an easy to understand set of rules, adding, updating or deleting state in Wörterbuch.

An application is described by an application manifest, which is itself stored on Wörterbuch, describing the kafka broker and the topics to be stored.

By storing kafka message offsets in Wörterbuch, kafka-to-worterbuch guarantees efficient at-least-once delivery while making it easy to replay topics from a specific offset by simply setting it on Wörterbuch and republishing the application manifest.