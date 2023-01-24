# -*- coding: utf-8 -*-

from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import glob
import os

SPARK_APP_NAME = "WordCountKafkaTwitter"
SPARK_CHECKPOINT_TMP_DIR = "tmp_spark_streaming"
SPARK_BATCH_INTERVAL_USER = 30
SPARK_BATCH_INTERVAL_WORD = 60
SPARK_LOG_LEVEL = "OFF"
APP_DATA_OUTPUT_DIR = "/home/ubuntu/BigData/tmp/output/wordcount"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "telegram-kafka"

def update_total_count(current_count, count_state):
    """
    Update the previous value by a new ones.

    Each key is updated by applying the given function
    on the previous state of the key (count_state) and the new values
    for the key (current_count).
    """
    if count_state is None:
        count_state = 0
    return sum(current_count, count_state)


def create_streaming_context(batch):
    """Create Spark streaming context."""
    sc = SparkContext(appName=SPARK_APP_NAME)
    sc.setLogLevel(SPARK_LOG_LEVEL)
    ssc = StreamingContext(sc, batch)
    ssc.checkpoint(SPARK_CHECKPOINT_TMP_DIR)
    return ssc

def word_sobst(mess):
    list_letter = []
    for i in range(ord('а'), ord('я')):
        list_letter.append(chr(i).upper())
    alphabet = tuple(list_letter)
    prep =(',','.','!','?')
    vow_letter = ('а', 'е', 'и',  'о',  'у',  'ы', 'э', 'ю', 'я')
    if mess.startswith(alphabet):
        if mess.endswith(prep):
            len_w = len(mess)
            mess = mess[0:len_w-1]
            if mess.endswith(vow_letter):
                len_w = len(mess)
                mess = mess[0:len_w - 1]
                return mess
            else:
                return mess

def parse_json_user(files):
    res = eval(files)
    return res["channel_name"]

def parse_json_word(files):
    res = eval(files)
    mess_m = res["message"]
    return mess_m

def create_stream(ssc):
    """
    Create subscriber (consumer) to the Kafka topic and
    extract only messages (works on RDD that is mini-batch).
    """
    return (
        KafkaUtils.createDirectStream(
            ssc, topics=[KAFKA_TOPIC],
            kafkaParams={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
            .map(lambda x: x[1])
    )

def cnt_user_60(batch):

    ssc = create_streaming_context(batch)
    messages = create_stream(ssc)
    total_counts_sorted_60 = (
        messages
            .map(parse_json_user)
            .map(lambda word: (word, 1))
            .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 60, 30, 10)
            .transform(lambda x_rdd: x_rdd.sortBy(lambda x: -x[1]))
    )
    total_counts_sorted_60.pprint()

    ssc.start()

    ssc.awaitTermination()

def cnt_user_600(batch):

    ssc = create_streaming_context(batch)
    messages = create_stream(ssc)
    total_counts_sorted_600 = (
        messages
            .map(parse_json_user)
            .map(lambda word: (word, 1))
            .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 600, 30, 10)
            .transform(lambda x_rdd1: x_rdd1.sortBy(lambda x: -x[1]))
    )
    total_counts_sorted_600.pprint()

    ssc.start()

    ssc.awaitTermination()

def cnt_word(batch):

    try:
        ssc = create_streaming_context(batch)
        messages = create_stream(ssc)
        total_words = (
            messages
                .map(parse_json_word)
                .flatMap(lambda line: line.split()[1:])
                .map(word_sobst)
                .filter(lambda x: x is not None)
                .map(lambda word: (word, 1))
                .reduceByKey(lambda x1, x2: x1 + x2)
                .updateStateByKey(update_total_count)
        )

        total_words.transform(lambda x_rdd: x_rdd.sortBy(lambda x: -x[1]).coalesce(1)).saveAsTextFiles("/home/ubuntu/BigData/tmp/output/wordcount")

        ssc.start()

        ssc.awaitTermination()
    except KeyboardInterrupt:
        path_to_sch = "/home/ubuntu/BigData/tmp/output"
        res_sch = glob.glob(path_to_sch + "/wordcount-*")
        last_dir = max(res_sch, key=os.path.getctime)
        res_cnt = glob.glob(last_dir + "/part-0000*")
        top_words = res_cnt[0]
        i = 0
        with open(top_words, "r") as f:
            for line in f:
                print(line)
                i = i + 1
                if i == 10:
                    break

parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("--cnt_user_60", action='store_true', help="Count messages with window 60s")
group.add_argument("--cnt_user_600", action='store_true', help="Count messages with window 600s")
group.add_argument("--cnt_word", action='store_true', help="Listen channels")
args = parser.parse_args()

def main():
    if args.cnt_user_60:
        cnt_user_60(SPARK_BATCH_INTERVAL_USER)
    if args.cnt_user_600:
        cnt_user_600(SPARK_BATCH_INTERVAL_USER)
    if args.cnt_word:
        cnt_word(SPARK_BATCH_INTERVAL_WORD)

if __name__ == "__main__":
    main()


