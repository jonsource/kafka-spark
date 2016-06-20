#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: kafka_wordcount.py <zk> <topic>
 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart
 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      examples/src/main/python/streaming/kafka_wordcount.py \
      localhost:2181 test`
"""
from __future__ import print_function

import sys
import MySQLdb

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import TopicAndPartition
from pyspark.sql import SQLContext


def myPrint(msg):
    print("\n-------------------------------------------\n %s\n-------------------------------------------\n" % msg)

class saver(object):
    def __init__(self, sqlc):
        self.sqlc = sqlc
        self.connection = MySQLdb.connect(user='root', db='test', host="127.0.0.1", passwd="")
        self.cursor = self.connection.cursor()

    def saveRdd(self, rdd, moar=None):
        if not rdd.count():
            myPrint('Empty set - nothing to save!')
            return
        df = self.sqlc.createDataFrame(rdd, ['word', 'count'])
        list = df.collect()
        self.cursor.execute("BEGIN")
        for x in list:
            # x[0][0]=datum, x[0][1]=id, x[1]=imps
            que = "UPDATE test.impressions SET view_count = view_count + %s WHERE banner_id = %s AND view_date = \"%s\"" % (x[1], x[0][1], x[0][0])
            print(que)
            cnt = self.cursor.execute(que)
            if not cnt:
                que = "INSERT INTO test.impressions (banner_id, view_date, view_count) VALUES (%s, \"%s\", %s)" % (x[0][1], x[0][0], x[1])
                print(que)
                self.cursor.execute(que)
        myPrint("%s messages" % len(list))
        saveStartOffsets("impressions", self.cursor)
        self.cursor.execute("COMMIT")
        self.connection.commit()

    def saveStream(self, dStream):
        dStream.foreachRDD(lambda rdd: self.saveRdd(rdd))

offsetRanges = []

def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd

def printOffsetRanges(rdd):
    for o in offsetRanges:
        print("%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset))

def cutSeconds(time):
    time  = time[:-5]
    time +="00:00"
    return time

def parse(row):
    row = row.split(' ',3)
    date = str(row[1]) + " " + str(cutSeconds(row[2]))
    try:
        bannerId = int(row[3]) 
    except Exception:
        bannerId = 0    
    return ((date, bannerId), 1)

def getStartOffsets(task, topic, partitions):
    connection = MySQLdb.connect(user='root', db='test', host="127.0.0.1", passwd="")
    cursor = connection.cursor()

    que = 'SELECT `partition`, `offset` FROM `test`.`kafka_offsets` WHERE `task`="%s" AND `topic`="%s"' % (task, topic)
    print(que)
    cnt = cursor.execute(que)
    if not cnt:
        for p in range(partitions):
            que = 'INSERT INTO test.kafka_offsets (`task`,`topic`,`partition`,`offset`) VALUES ("%s","%s",%s,0)' % (task, topic, p)
            print(que)
            cnt = cursor.execute(que)
            connection.commit()
        return getStartOffsets(task, topic, partitions)
    ret = {}
    for row in cursor.fetchall():
	ret[TopicAndPartition(topic, row[0])] = long(row[1])
    connection.close()
    return ret

def saveStartOffsets(task, cursor):
    global offsetRanges
    for o in offsetRanges:
        print("%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset))
        que = 'UPDATE test.kafka_offsets SET `offset` = %s WHERE `task`="%s" AND `topic`="%s" AND `partition`=%s' % (o.untilOffset, task, o.topic, o.partition)
        print(que)
        cnt = cursor.execute(que)
        # if not cnt:
        #    que = 'INSERT INTO test.kafka_offsets (`task`,`topic`,`partition`,`offset`) VALUES ("%s","%s",%s,%s)' % (task, o.topic, o.partition, o.untilOffset)
        #    print(que)
        #    cnt = cursor.execute(que)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 5)
    sqlc = SQLContext(sc)

    kafkaList, topic = sys.argv[1:]
    offsets = getStartOffsets("impressions", topic, 12)
    print(offsets)
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": kafkaList}, fromOffsets=offsets)
    #kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 12})
    kvs.transform(storeOffsetRanges).foreachRDD(printOffsetRanges)
    lines = kvs.map(lambda x: x[1])
    pairs  = lines.map(parse)
    # format je (('2016-01-28 14:06:00', 999), 6)
    counts = pairs.reduceByKey(lambda a, b: a+b)
    counts.pprint()

    s = saver(sqlc)
    s.saveStream(counts)
    # df = sqlc.createDataFrame(counts, ['word', 'count']);
    # print(df)


    ssc.start()
    ssc.awaitTermination()
