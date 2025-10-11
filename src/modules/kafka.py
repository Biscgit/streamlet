# """This file contains modules for Kafka requests."""
#
# import json
# import logging
# import threading
# from collections import deque
#
# from confluent_kafka import Consumer, KafkaException, Producer
# from voluptuous import Optional, Schema
#
# from core.abstract import AbstractInput, AbstractOutput
# from core.modules import ImportableModule
# from core.validation import EnvironmentVar, OneOf
#
#
# @ImportableModule
# class KafkaInput(AbstractInput):
#     """An input module that retrieves data from a Kafka topic."""
#
#     def __init__(self, *args):
#         super().__init__(*args)
#         self._config = {
#             "bootstrap.servers": self.connection_config["servers"],
#             "group.id": self.connection_config["group_id"],
#             "auto.offset.reset": self.connection_config["offset_reset"],
#         }
#         self._consumer: Consumer | None = None
#         self.topic_cache = {x["params"]["topic"]: deque() for x in self.task_confs}
#
#         self._poll = threading.Thread(target=self.kafka_poll_thread, args=(self,))
#         self._poll_running = False
#         self._poll_lock = threading.Lock()
#
#     def connect(self) -> None:
#         topics = list(self.topic_cache.keys())
#         logging.info(
#             "Connecting Kafka Consumer %s and subscribing to %d Kafka topics.",
#             *(self.name, len(topics)),
#         )
#
#         self._consumer = Consumer(self._config)
#         self._consumer.subscribe(topics)
#
#         logging.info("Starting Kafka Consumer %s polling thread.", self.name)
#         self._poll.start()
#
#     def kafka_poll_thread(self) -> None:
#         """Polls the Kafka consumer for messages."""
#         self._poll_running = True
#         while self._poll_running:
#             try:
#                 if not (msg := self._consumer.poll(1)):
#                     continue
#
#                 if err := msg.error():
#                     logging.error("Consumer error: [%s] %s", err.name(), err.str())
#                     continue
#
#                 with self._poll_lock:
#                     topic = msg.topic()
#                     self.topic_cache[topic].append(msg.value())
#
#             except KafkaException as e:
#                 logging.error("Kafka consumer error: [%s] %s", type(e), e)
#
#     @staticmethod
#     def connection_schema() -> Schema:
#         return Schema(
#             {
#                 "servers": EnvironmentVar(),
#                 "group_id": EnvironmentVar(),
#                 Optional("offset_reset", default="earliest"): OneOf(["earliest", "latest"]),
#             }
#         )
#
#     @staticmethod
#     def task_params_schema() -> Schema:
#         return Schema({Optional("topic"): EnvironmentVar()})
#
#     def __call__(self, **kwargs):
#         topic = kwargs["topic"]
#         queue = self.topic_cache[topic]
#
#         with self._poll_lock:
#             data = queue.copy()
#             queue.clear()
#
#         if not data:
#             return None
#
#         return [json.loads(d.decode("utf-8")) for d in data]
#
#     def pre_shutdown(self):
#         """This function is used to prepare the module for shutdown, e.g., flushing buffers."""
#         logging.info("Shutting down and flushing Kafka client for %s.", self.name)
#         self._consumer.close()
#         self._poll_running = False
#
#         for t in self.tasks:
#             t.celery_task.apply()
#
#     def shutdown(self) -> None:
#         logging.info("Cleaning up Kafka polling thread for %s.", self.name)
#         self._poll.join(timeout=10)
#
#         for t, q in self.topic_cache.items():
#             if l := len(q):
#                 logging.error("Failed to flush %d from topic %s", l, t)
#
#
# @ImportableModule
# class KafkaOutput(AbstractOutput):
#     """An output module that sends data to a Kafka topic."""
#
#     def __init__(self, *args):
#         super().__init__(*args)
#         self._config = {"bootstrap.servers": self.connection_config["servers"]}
#         self._producer: Producer | None = None
#         self.topic = self.params["topic"]
#
#     def connect(self) -> None:
#         logging.info("Connecting %s Kafka Producer.", self.name)
#         self._producer = Producer(self._config)
#
#     @staticmethod
#     def connection_schema() -> Schema:
#         return Schema({"servers": EnvironmentVar()})
#
#     @classmethod
#     def params_schema(cls) -> Schema:
#         return Schema({"topic": EnvironmentVar()})
#
#     @staticmethod
#     def kafka_callback(err, _msg):
#         """Callback for Kafka producer errors."""
#         if err:
#             logging.error("Kafka producer error: [%s] %s", err.name(), err.str())
#
#     def __call__(self, data):
#         for message in data:
#             message = json.dumps(message).encode("utf-8")
#
#             self._producer.poll(0)
#             self._producer.produce(self.topic, message, callback=self.kafka_callback)
#
#         self._producer.flush()
#
#     def shutdown(self) -> None:
#         logging.info("Flushing Kafka producer for output %s.", self.name)
#         self._producer.flush()
