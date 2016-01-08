#!/usr/bin/python

from pykafka import KafkaClient
import sys
import time

if len(sys.argv) != 3:
	print "Usage: producer.py <kafka_host> <topic>"
	exit(-1)

kafkaHost, topic = sys.argv[1:]
client = KafkaClient(hosts=kafkaHost)

topicTest = client.topics[topic]
testProducer = topicTest.get_producer()

count = 0

print "Connection established. Sending messages.\n(. = 10 messages)"

while(1):
	testProducer.produce('test message %s' % count)
	count = count+1
	if count % 10:
		sys.stdout.write('.')
		sys.stdout.flush()
	time.sleep(0.7)

