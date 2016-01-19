#!/usr/bin/python

from pykafka import KafkaClient
import sys
import time

if len(sys.argv) < 3:
	print "Usage: producer.py <kafka_host> <topic> [benchmark]"
	exit(-1)

kafkaHost, topic = sys.argv[1:3]
print kafkaHost, topic
client = KafkaClient(hosts=kafkaHost)

topicTest = client.topics[topic]

def benchmark(producer, n=1000):
	start = time.time()
	count = 0
	print "\nbenchmark %s : " % n,
	while(count < n):
		producer.produce('test message %s' % count)
		if not count % 10000:
			sys.stdout.write('.')
			sys.stdout.flush()
		count += 1
	producer.stop()
	end = time.time()
	print end - start
	return start, end

if len(sys.argv) > 3 and sys.argv[3] == 'benchmark':
	number = 300000
	pre = benchmark(topicTest.get_producer(linger_ms=1, min_queued_messages=1), number)
	normal = benchmark(topicTest.get_producer(linger_ms=11000, min_queued_messages=20), number)
	ack = benchmark(topicTest.get_producer(linger_ms=11000, min_queued_messages=20, required_acks=1), number)
	ack = benchmark(topicTest.get_producer(linger_ms=11000, min_queued_messages=20, required_acks=2), number)

	# print "\n*****"
	# print pre[1]-pre[0]
	# print normal[1]-normal[0]
	# print ack[1]-ack[0]
else:
	print "Connection established (using pykafka). Sending messages.\n(. = 10 messages)"
	testProducer = topicTest.get_producer(linger_ms=11000, min_queued_messages=20)
	count = 0
	while(1):
		testProducer.produce('test message %s' % count)
		count = count+1
		if not count % 10:
			sys.stdout.write('.')
			sys.stdout.flush()
		time.sleep(0.7)

