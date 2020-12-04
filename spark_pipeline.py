from pyspark import SparkContext, SparkConf
from math import log
import csv

# Creates SparkContext, reads sequenceFile and gets count
sc = SparkContext(appName="big_data_project")
rdd = sc.sequenceFile("part-00000")
N = rdd.count()

# Words analyzed
word1 = "coca-cola"
word2 = "pepsi"

# Document counter
def doc_counter(item):
    words_raw = item[1].strip().split()
    words = [
        i.lower()
        for i in words_raw
        if (not any(j.isdigit() for j in i) and len(i) > 3 and (not ("——") in i))
    ]

    if option_picked == 0:
        if word1 in words and word2 in words:
            return [(i, 1) for i in set(words)]
    elif option_picked == 1:
        if word1 in words and word2 not in words:
            return [(i, 1) for i in set(words)]
    else:
        if word1 not in words and word2 in words:
            return [(i, 1) for i in set(words)]

    return []


# Word counter
def word_counter(item):
    words_raw = item[1].strip().split()
    words = [
        i.lower()
        for i in words_raw
        if (not any(j.isdigit() for j in i) and len(i) > 3 and (not ("——") in i))
    ]

    if option_picked == 0:
        if word1 in words and word2 in words:
            return [(i, 1) for i in words]
    elif option_picked == 1:
        if word1 in words and word2 not in words:
            return [(i, 1) for i in words]
    else:
        if word1 not in words and word2 in words:
            return [(i, 1) for i in words]
    return []


# Sums result and item to get number of owrds
def count_words(result, item):
    return result + item


# option_picked represents which words you want to analyze
# 0 = coca-cola and pepsi
# 1 = coca-cola
# 2 = pepsi

option_picked = 0
docs_combined = rdd.flatMap(doc_counter).reduceByKey(count_words)
words_combined = rdd.flatMap(word_counter).reduceByKey(count_words)

option_picked = 1
docs_coca = rdd.flatMap(doc_counter).reduceByKey(count_words)
words_coca = rdd.flatMap(word_counter).reduceByKey(count_words)

option_picked = 2
docs_pepsi = rdd.flatMap(doc_counter).reduceByKey(count_words)
words_pepsi = rdd.flatMap(word_counter).reduceByKey(count_words)

# Calculates idf value
def calc_idf(item):
    return item[0], log(N / item[1], 10)


# Calculates frequency
def calc_frequency(item):
    return item[0], log(1 + item[1], 10)


# Calculates relevancy
def calc_relevancy(item):
    return item[0], item[1][0] * item[1][1]


# Here we map the functions created to the documents and to the words in order to calculate relevancy
# Then, we generate a .csv file with the top 100 words ordered by relevancy for each scenario (using csv module)

rdd_idf_combined = docs_combined.map(calc_idf)
rdd_freq_combined = words_combined.map(calc_frequency)
rdd_relevancy_combined = rdd_idf_combined.join(rdd_freq_combined).map(calc_relevancy)

top_100_combined = rdd_relevancy_combined.takeOrdered(100, key=(lambda x: -x[1]))
with open("csv/soda.csv", "w") as soda:
    csv_out = csv.writer(soda)
    csv_out.writerow(["word", "freq"])
    for row in top_100_combined:
        csv_out.writerow(row)


rdd_idf_coca = docs_coca.map(calc_idf)
rdd_freq_coca = words_coca.map(calc_frequency)
rdd_relevancy_coca = rdd_idf_coca.join(rdd_freq_coca).map(calc_relevancy)

top_100_coca = rdd_relevancy_coca.takeOrdered(100, key=(lambda x: -x[1]))
with open("csv/coca.csv", "w") as coca:
    csv_out = csv.writer(coca)
    csv_out.writerow(["word", "freq"])
    for row in top_100_coca:
        csv_out.writerow(row)


rdd_idf_pepsi = docs_pepsi.map(calc_idf)
rdd_freq_pepsi = words_pepsi.map(calc_frequency)
rdd_relevancy_pepsi = rdd_idf_pepsi.join(rdd_freq_pepsi).map(calc_relevancy)

top_100_pepsi = rdd_relevancy_pepsi.takeOrdered(100, key=(lambda x: -x[1]))
with open("csv/pepsi.csv", "w") as pepsi:
    csv_out = csv.writer(pepsi)
    csv_out.writerow(["word", "freq"])
    for row in top_100_pepsi:
        csv_out.writerow(row)