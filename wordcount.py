import nltk


from pyspark import SparkContext, SparkConf
from operator import add
import string
import os
import collections
import random

from nltk.tokenize import word_tokenize
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem.porter import *

import sys

APP_NAME = "Word Count"


def word_count(sc, base_location, folder_names):

    features = set()

    for folder_name in folder_names:
        textRDD = sc.textFile(base_location+folder_name)
        stemmer = PorterStemmer()
        stop_words = stopwords.words('english')
        custom_stop_words = ["could", "one", "ms", "mr", "follow", "said", "ha", "wa", "would", "thi", "hi", "like", "also", "say", "take"]
        stop_words = stop_words + custom_stop_words
        punctuation = string.punctuation

        words = textRDD.flatMap(lambda s: word_tokenize(s.lower()))

        stems = words.map(lambda w: stemmer.stem(w))\
            .filter(lambda p: p not in punctuation)\
            .filter(lambda p: re.sub(r'[^\x00-\x7f]', r'', p))\
            .filter(lambda s: s not in stop_words)\
            .filter(lambda a: a.isalpha() == True) \
            .map(lambda x: (x.encode('ascii', 'ignore'), 1))
        wordcount = stems.reduceByKey(add).sortBy(lambda a: a[1], ascending=False).take(40)
        for (word, count) in wordcount:
            features.add(word)

    feature_file = sc.parallelize(features)
    feature_file.coalesce(1).saveAsTextFile("features")
    print(features)

    return features

def extract_feature(sc, base_location, folder_names, features):
    doc_id = -1
    input_matrix = sc.parallelize([])
    for folder in folder_names:
        doc_id = doc_id + 1

        textRDD = sc.wholeTextFiles(base_location+folder)
        words = textRDD.flatMap(customSomething)\
                .map(lambda (k, v): ((k, v.encode('ascii', 'ignore')), 1))
        wordcount = words.reduceByKey(add)\
                    .map(lambda (k,v): (str(k[0]), str(features[str(k[1])]) +":"+ str(v))) \
                    .sortBy(lambda (k, v): int(v.split(":")[0]), ascending = True) \
                    .reduceByKey(lambda v1, v2: v1+" "+v2)\
                    .map(lambda (k,v): str(doc_id)+ " "+v)
        input_matrix = input_matrix.union(wordcount)

    input_matrix.coalesce(1).saveAsTextFile("output")


def customSomething((file_name, article)):
    word_list = word_tokenize(article.lower())
    output = []
    stemmer = PorterStemmer()

    for word in word_list:
        stemmed = stemmer.stem(word)
        if(stemmed in features):
            output.append((file_name, stemmed))

    return output


if __name__ == "__main__":
    conf = SparkConf().setAppName(APP_NAME).setMaster("local[*]")
    sc = SparkContext(conf=conf)
    base_location = sys.argv[1]

    folder_names = []
    folder_names.append(sys.argv[2])
    folder_names.append(sys.argv[3])
    folder_names.append(sys.argv[4])
    folder_names.append(sys.argv[5])

    features = word_count(sc, base_location, folder_names)
    dic = dict()

    i = 1
    for feature in features:
        dic[feature] = i
        i = i + 1

    extract_feature(sc, base_location, folder_names, dic)



