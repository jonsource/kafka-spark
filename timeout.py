#!/usr/bin/python
# coding=utf-8
import time
from threading import Thread
import signal
import errno
from functools import wraps
from logni import log
import os


class TimeoutError(Exception):
	pass


def timeout(seconds=30, error_message=os.strerror(errno.ETIME)):

	def decorator(func):

		def handleTimeout(signum, frame):
			raise TimeoutError(error_message)

		def wrapper(*args, **kwargs):
			signal.signal(signal.SIGALRM, handleTimeout)
			signal.alarm(seconds)
			try:
				result = func(*args, **kwargs)
			finally:
				signal.alarm(0)
			return result

		return wraps(func)(wrapper)

	return decorator


class TimeoutByThreads(object):

	def __init__(self, seconds=1):
		self.finished = False
		self.timeout = seconds
		self.worker = None
		self.timer = 0
		self.result = None
		self.timeout_step = None

	def work(self, *args, **kwargs):
		self.result = self.method(*args, **kwargs)
		self.finished = True
		return self.result

	def __call__(self, method):
		def wrapper(*args, **kwargs):
			self.thread = Thread(target=self.work, args=args, kwargs=kwargs)
			self.thread.start()

			# if not self.timeout_step:
			# 	self.timeout_step = self.timeout / 20.0
			#
			# while self.thread and self.thread.is_alive() and not self.finished and self.timer < self.timeout:
			# 	time.sleep(self.timeout_step)
			# 	self.timer += self.timeout_step
			# if not self.finished:
			# 	log.ni("TimeoutError - Connecting to kafka", ERR=4)
			# 	#raise TimeoutError()

			self.thread.join(self.timeout)
			if self.thread.is_alive():
				log.ni("TimeoutError - Connecting to kafka", ERR=4)
				raise TimeoutError()

			return self.result
		self.method = method
		return wrapper
