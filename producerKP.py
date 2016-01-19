#!/usr/bin/python

from kafka import SimpleProducer, KafkaClient
import sys
import time

if len(sys.argv) != 3:
	print "Usage: producer.py <kafka_host> <topic>"
	exit(-1)

kafkaHost, topic = sys.argv[1:]

client = KafkaClient(hosts=kafkaHost)
producer = SimpleProducer(client, async=True, batch_send_every_n=20, batch_send_every_t=11)

count = 0

print "Connection established (using kafka-python). Sending messages.\n(. = 10 messages)"

while(1):
	producer.send_messages(topic, 'test message %s' % count)
	count += 1
	if count % 10:
		sys.stdout.write('.')
		sys.stdout.flush()
	time.sleep(0.7)
