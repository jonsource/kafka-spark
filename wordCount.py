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
from pyspark.sql import SQLContext


def myPrint(msg):
    print("\n-------------------------------------------\n %s\n-------------------------------------------\n" % msg)

class saver(object):
    def __init__(self, sqlc):
        self.sqlc = sqlc
        self.connection = MySQLdb.connect(user='root', db='test', host="172.17.0.1", passwd="1234")
        self.cursor = self.connection.cursor()

    def saveRdd(self, rdd, moar=None):
        if not rdd.count():
            myPrint('Empty set - nothing to save!')
            return
        df = self.sqlc.createDataFrame(rdd, ['word', 'count'])
        # df.write.jdbc(
        #     url="jdbc:postgresql://[hostname]/[database]?user=[username]&password=[password]",
        #     dbtable="pubs",
        #     mode="overwrite",
        # )
        list = df.collect()
        for x in list:
            que = "UPDATE test.impressions SET view_count = view_count + %s WHERE banner_id = %s AND view_date = DATE_FORMAT(NOW(), '%%Y-%%m-%%d %%H:00:00')" % (x[1], x[0])
            print(que)
            cnt = self.cursor.execute(que)
            if not cnt:
                que = "INSERT INTO test.impressions (banner_id, view_date, view_count) VALUES (%s, DATE_FORMAT(NOW(), '%%Y-%%m-%%d %%H:00:00'), %s)" % (x[0], x[1])
                print(que)
                self.cursor.execute(que)
        myPrint("%s messages" % len(list))
        self.connection.commit()

    def saveStream(self, dStream):
        dStream.foreachRDD(lambda rdd: self.saveRdd(rdd))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 5)
    sqlc = SQLContext(sc)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.replace("view ", '').split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    s = saver(sqlc)
    s.saveStream(counts)
    # df = sqlc.createDataFrame(counts, ['word', 'count']);
    # print(df)


    ssc.start()
    ssc.awaitTermination()