from pyspark import SparkContext
from functools import partial
from math import log10
import csv
import os

########################### FUNCTIONS ###########################


def process_rdd(item: tuple):
    return (item[0], item[1].lower().strip())


def word_filter(item: tuple):
    return item[0].isalpha() and len(item[0]) >= 3


def find_word(word: str, item: tuple):
    is_in = False if word not in item[1] else True
    return is_in


def make_tuples(item: tuple):
    return [(word, 1) for word in item[1].split()]


def make_unique_tuples(item: tuple):
    return [(word, 1) for word in set(item[1].split())]


def calc_frequency(item: tuple):
    return (item[0], log10(1 + item[1]))


def calc_idf(n: int, item: tuple):
    return (item[0], log10(n / item[1]))


def calc_relevancy(item: tuple):
    return (item[0], item[1][0] * item[1][1])


def filter_frequency(n: int, item: tuple):
    return ((0.7 * n) > item[1]) and (item[1] >= 5)


########################### MAIN ###########################

word1 = "volkswagen"
word2 = "hyundai"

sc = SparkContext(appName="APS2")
rdd = sc.sequenceFile("part-00000")
rdd = rdd.map(process_rdd)
N = rdd.count()

rdd_idf = (
    rdd.flatMap(make_unique_tuples)
    .reduceByKey(lambda x, y: (x + y))
    .filter(word_filter)
    .filter(partial(filter_frequency, N))
    .map(partial(calc_idf, N))
)

rdd_w1 = rdd.filter(partial(find_word, word1))
rdd_w2 = rdd.filter(partial(find_word, word2))
rdd_both = rdd_w1.intersection(rdd_w2)


# Calculating top 100 for both words

rdd_both_freq = (
    rdd_both.flatMap(make_tuples)
    .reduceByKey(lambda x, y: x + y)
    .filter(word_filter)
    .map(calc_frequency)
)
rdd_both_rel = rdd_both_freq.join(rdd_idf).map(calc_relevancy)
top_100_both = rdd_both_rel.takeOrdered(100, key=lambda x: -x[1])

# Calculating top 100 for word 1 only

rdd_w1_only = rdd_w1.subtractByKey(rdd_w2)
rdd_w1_only_freq = (
    rdd_w1_only.flatMap(make_tuples)
    .reduceByKey(lambda x, y: x + y)
    .filter(word_filter)
    .map(calc_frequency)
)
rdd_w1_only_rel = rdd_w1_only_freq.join(rdd_idf).map(calc_relevancy)
top_100_w1_only = rdd_w1_only_rel.takeOrdered(100, key=lambda x: -x[1])

# Calculating top 100 for word 2 only

rdd_w2_only = rdd_w2.subtractByKey(rdd_w1)
rdd_w2_only_freq = (
    rdd_w2_only.flatMap(make_tuples)
    .reduceByKey(lambda x, y: x + y)
    .filter(word_filter)
    .map(calc_frequency)
)
rdd_w2_only_rel = rdd_w2_only_freq.join(rdd_idf).map(calc_relevancy)
top_100_w2_only = rdd_w2_only_rel.takeOrdered(100, key=lambda x: -x[1])


# Checks if dir exists; if it doesn't, creates it
if not os.path.isdir("csv"):
    os.mkdir("csv")
else:
    for item in os.listdir("csv"):
        if item.endswith(".csv"):
            os.remove(os.path.join("csv", item))

# Writes all top 100 in different csv files

with open(f"csv/{word1}&&{word2}.csv", "w") as both:
    csv_out = csv.writer(both)
    csv_out.writerow(["word", "freq"])
    for row in top_100_both:
        csv_out.writerow(row)


with open(f"csv/{word1}.csv", "w") as w1:
    csv_out = csv.writer(w1)
    csv_out.writerow(["word", "freq"])
    for row in top_100_w1_only:
        csv_out.writerow(row)


with open(f"csv/{word2}.csv", "w") as w2:
    csv_out = csv.writer(w2)
    csv_out.writerow(["word", "freq"])
    for row in top_100_w2_only:
        csv_out.writerow(row)
