from pyspark import SparkContext, SparkConf

sc = SparkContext(appName="big_data_project")
data = sc.sequenceFile("part-00000")


def document_counter(item):
    words_brute = item[1].strip().split()
    words = [
        i.lower()
        for i in words_brute
        if (not any(j.isdigit() for j in i) and len(i) > 3 and (not ("——") in i))
    ]

    if scope == 0:
        if "coca" in words and "pepsi" in words:
            return [(i, 1) for i in set(words)]
    elif scope == 1:
        if "coca" in words and "pepsi" not in words:
            return [(i, 1) for i in set(words)]
    else:
        if "coca" not in words and "pepsi" in words:
            return [(i, 1) for i in set(words)]

    return []


def word_counter(item):
    words_brute = item[1].strip().split()
    words = [
        i.lower()
        for i in words_brute
        if (not any(j.isdigit() for j in i) and len(i) > 3 and (not ("——") in i))
    ]

    if scope == 0:
        if "coca" in words and "pepsi" in words:
            return [(i, 1) for i in words]
    elif scope == 1:
        if "coca" in words and "pepsi" not in words:
            return [(i, 1) for i in words]
    else:
        if "coca" not in words and "pepsi" in words:
            return [(i, 1) for i in words]
    return []


def count_words(result, item):
    return result + item


# Variable scope -> 0 = Words together, 1 = coca alone, 2 = pepsi alone
scope = 0
docs_together = data.flatMap(document_counter).reduceByKey(count_words)
words_together = data.flatMap(word_counter).reduceByKey(count_words)

scope = 1
docs_coca = data.flatMap(document_counter).reduceByKey(count_words)
words_coca = data.flatMap(word_counter).reduceByKey(count_words)

scope = 2
docs_pepsi = data.flatMap(document_counter).reduceByKey(count_words)
words_pepsi = data.flatMap(word_counter).reduceByKey(count_words)

print(docs.filter(lambda a: a[0] == "coca").collect())
print(docs.filter(lambda a: a[0] == "pepsi").collect())
print(words.filter(lambda a: a[0] == "coca").collect())
print(words.filter(lambda a: a[0] == "pepsi").collect())