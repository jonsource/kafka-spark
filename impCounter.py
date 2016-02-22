#!/usr/bin/python
# coding=utf-8
import MySQLdb
import datetime
import time
import sys
import argparse
import pykafka
from kafkaConnector import plugLogging
from timeout import TimeoutByThreads, TimeoutError

parser = argparse.ArgumentParser(description='Get kafka data.')
parser.add_argument('interval', metavar='I', type=float, help='interval')
parser.add_argument('--pass', dest='passwd', help='db password', default="")
parser.add_argument('--table', dest='table', help='db table, defaults to "impressions"', default="impressions")
parser.add_argument('--file', dest='file', help='output file')
parser.add_argument('--id', dest='id', type=int, help='table row id')
parser.add_argument('--max-time', dest='max_same_time', type=int, default = 300, help='no change in table value timeout in seconds')
parser.add_argument('--kafka', dest='kafka_client', help='kafka client(s) ip:port, coma separated list')
parser.add_argument('--kafka-topic', dest='kafka_topic', default='test', help='number of kafka brokers used')
parser.add_argument('-v', dest='verbose', action='store_const', const=True, help='show pykafka logs')

args = parser.parse_args()

connection = MySQLdb.connect(user='root', db='test', host="127.0.0.1", passwd=args.passwd)
cursor = connection.cursor()

if args.verbose:
    plugLogging()

if not args.id:
	try:
	        que = "SELECT MAX( id ) FROM `%s`" % (args.table)
        	cursor.execute(que)
	        args.id = int(cursor.fetchone()[0])
	        print args.id
	except:
		args.id = None

if args.id:
	que = "SELECT `view_count` FROM `%s` WHERE `id`=%s" % (args.table, args.id)

if(args.file):
        outfile = open(args.file, 'w')

same_steps = 10
max_same_time = args.max_same_time
last_val = 0
last_same_time = time.time()

class kafkaConnection(object):

    def __init__(self, hosts, topic):
        self.hosts = hosts
        self.topic = topic
        self.last_offsets = None
        self.connected = False
        self.kafka_client = None
        self.total = 0

    def connect(self):
        self.kafka_client = pykafka.KafkaClient(hosts=self.hosts, socket_timeout_ms=500, offsets_channel_socket_timeout_ms=10 * 500)
        self.kafka_topic = self.kafka_client.topics[self.topic]
        self.connected = True

    def get_offsets_delta(self, old, new):
        if not old or (len(old) != len(new)):
                return 0
        delta = 0;

        for i in range(0, len(old)):
            delta += new[i].offset[0] - old[i].offset[0]

        return delta


    def get_offsets_data(self):
        try:
            new_offsets = self.kafka_topic.latest_available_offsets()
        except pykafka.exceptions.SocketDisconnectedError:
            self.kafka_client.cluster.update()
            return 0
        try:
            ret = self.get_offsets_delta(self.last_offsets, new_offsets)
        except IndexError:
            self.last_offsets = new_offsets
            return 0
        self.last_offsets = new_offsets
        return ret

    def get_active_brokers(self):
        leaders = []
        replicas = []
        for p in self.kafka_topic.partitions.values():
            if p.leader.id not in leaders:
                leaders.append(p.leader.id)
            for r in p.replicas:
                if r.id not in replicas:
                    replicas.append(r.id)
        return leaders, replicas

    def get_kafka_data(self):
        if not self.connected:
            self.connect()
            return "connecting.."

        #try:
        active_brokers = self.get_active_brokers()
        delta = self.get_offsets_data()
        self.total += delta

        #except pykafka.exceptions.SocketDisconnectedError:
        #    self.connected = False
        #    return "disconnected .."

        self.kafka_client.cluster.update()

        return "%s Δ: %4s E: %5s " % (active_brokers, delta, self.total)


if args.kafka_client:
    kafka_connection = kafkaConnection(args.kafka_client, args.kafka_topic)

while(1):
	cursor.execute(que)
        connection.commit()
	res = cursor.fetchone()
	if not res or not res[0]:	
		res = (0,)
	
        now = time.time()
        td = now - last_same_time
        if(res[0] == last_val):
                if(td > max_same_time):
                        break
        else:
                last_val = res[0]
                last_same_time = now

        sto = (res[0]/100000)*'#'
        ten = ((res[0]%100000)/2000)*'.'
        sl = min(10, int(td / max_same_time * same_steps))

        same = sl*'.'+(same_steps-sl)*' '
        t = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        kafka = ""
        if args.kafka_client:
                kafka = kafka_connection.get_kafka_data()

        print t+" ["+same+"] "+kafka+"- "+str(res[0])+" "+sto+ten
        if args.file:
                outfile.write("%s %s \n" % (t, res[0]))
        time.sleep(args.interval)

if args.file:
        outfile.close()
