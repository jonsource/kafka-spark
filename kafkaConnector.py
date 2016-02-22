#!/usr/bin/python
#coding=utf-8

import logging
from logni import log
from timeout import timeout
from timeout import TimeoutByThreads
from pykafka import KafkaClient
import pykafka
import sys
import traceback


def transformLoggerLevel(level):

	lvlMap = {'DEBUG': ('DBG', 3),
     'WARNING': ('WARN', 3),
     'ERROR': ('ERR', 4),
     'INFO': ('INFO', 3),
     'EXCEPTION': ('ERR', 4),
     'Level 5': ('INFO', 2)
	}

	if level in lvlMap:
		return lvlMap[level]
	log.ni("Unknown log level %s", level, INFO=3)
	return 'ERR', 3


def createLogniAdapter(module, method=False):

	if module:
		module = module+': '
	else:
		module = ''

	def loggingLogniAdapter(level, msg, *args, **kwargs):
		lvlName, lvlVal = transformLoggerLevel(logging.getLevelName(level))
		kwargs[lvlName] = lvlVal
		log.ni("%s%s" % (module, msg), *args, offset=3, **kwargs)

	def loggingLogniAdapterMethod(self, level, msg, *args, **kwargs):
		loggingLogniAdapter(level, msg, args, kwargs)

	if(method):
		return loggingLogniAdapterMethod
	else:
		return loggingLogniAdapter

def plugLogging():
	logging.getLogger("pykafka").setLevel(1)

	logging.Logger.log = createLogniAdapter('', method=True)

	for name in ('pykafka.cluster', 'pykafka.broker', 'pykafka.handlers', 'pykafka.producer', 'pykafka.topic', 'pykafka.connection', 'pykafka.partition'):
		module = sys.modules[name]
		module.log._log = createLogniAdapter('pykafka')

	logging.info("Starting log")
	log.stderr(1)
	log.mask('I1W1E1D1F1')

class KafkaProducerUnavailable(Exception):
	pass

class KafkaConnector(object):

	def __init__(self, config):
		self.config = config
		self.kafkaProducer = None
		self._getKafkaProducer()

	#@timeout(seconds=2)
	@TimeoutByThreads(seconds=0.6)
	def _connectKafka(self):
		log.ni("KafkaConnector: Connecting to kafka at %s ...", (self.config.get("kafka", "zk_hosts"),), WARN=4)

		# debugging only - fake connection latency
		# sleep = 1 + random.random()*1
		# print "sleep ", sleep
		# time.sleep(sleep)

		try:
			self.kafkaClient = KafkaClient(zookeeper_hosts=self.config.get("kafka", "zk_hosts"), socket_timeout_ms=500, offsets_channel_socket_timeout_ms=10 * 500)
			self.kafkaTopic = self.kafkaClient.topics[self.config.get("kafka", "topic")]
			self.kafkaProducer = self.kafkaTopic.get_producer(linger_ms=int(self.config.get("kafka", "lingerTimeMs")), min_queued_messages=int(self.config.get("kafka", "minBatchSize")))
			log.ni("KafkaConnector: got one", INFO=1)
		except Exception as e:
			log.ni("KafkaConnector: didn't find one %s", (traceback.print_exc(),), WARN=4)

	def _getKafkaProducer(self):
		if not self.kafkaProducer:
			log.ni("KafkaConnector: no kafka producer", INFO=1)
			try:
				self._connectKafka()
			except Exception as e:
				log.ni("KafkaConnector: didn't get producer %s", (traceback.print_exc(),), WARN=4)

			if not self.kafkaProducer:
				# raise KafkaProducerUnavailable
				return None

			log.ni("KafkaConnector: got kafka producer", INFO=1)
		return self.kafkaProducer

	def sendToKafka(self, message):
		# try:
		# 	log.ni("KafkaConnector * Send to kafka: %s", (message,), INFO=2)
		# 	self._getKafkaProducer().produce(message)
		# except Exception as e:
		# 	log.ni("KafkaConnector * didn't send %s", (e,), WARN=4)
		if self.kafkaProducer:
			self.kafkaProducer.produce(message)
		else:
			log.ni("KafkaConnector: sending %s without producer", (message,), ERR=2)

	def stopProducer(self):
		if self.kafkaProducer:
			self.kafkaProducer.stop()
