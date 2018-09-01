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

APP_NAME = "Extract Article"
def extract_feature(sc, file_location, features, featurefile):
    # doc_id = -1
    # input_matrix = sc.parallelize([])
    # for folder in folder_names:
    #     doc_id = doc_id + 1
    #
    #     textRDD = sc.wholeTextFiles(base_location+folder)
    #     words = textRDD.flatMap(customSomething)\
    #             .map(lambda (k, v): ((k, v.encode('ascii', 'ignore')), 1))
    #     wordcount = words.reduceByKey(add)\
    #                 .map(lambda (k,v): (str(k[0]), str(features[str(k[1])]) +":"+ str(v))) \
    #                 .sortBy(lambda (k, v): int(v.split(":")[0]), ascending = True) \
    #                 .reduceByKey(lambda v1, v2: v1+" "+v2)\
    #                 .map(lambda (k,v): str(doc_id)+ " "+v)
    #     input_matrix = input_matrix.union(wordcount)
    #
    # input_matrix.coalesce(1).saveAsTextFile("output")

    article = sc.wholeTextFiles(file_location)
    files = article.map(lambda (file, content): file)
    feature_words = files.cartesian(featurefile)

    feature_words = feature_words \
        .map(lambda (file, word): ((file, features[word]), 0))

    words = article.flatMap(customSomething) \
                    .map(lambda (k, v): ((k, features[v]), 1))
    wordcount = words.union(feature_words)\
                .reduceByKey(add)

    output_feature = wordcount\
                .map(lambda (k,v): (str(k[0]), str(k[1]) +":"+ str(v))) \
                .sortBy(lambda (k, v): int(v.split(":")[0]), ascending = True) \
                .reduceByKey(lambda v1, v2: v1+" "+v2)\
                .map(lambda (k,v): str(0)+ " "+v)

    output_feature.coalesce(1).saveAsTextFile("classified")
    sc.stop()


def customSomething((file_name, article)):
    word_list = word_tokenize(article.lower())
    output = []
    stemmer = PorterStemmer()

    for word in word_list:
        stemmed = stemmer.stem(word)
        if(stemmed in features):
            output.append((file_name, stemmed))

    return output

# Input: article location, feature file
if __name__ == "__main__":
    conf = SparkConf().setAppName(APP_NAME).setMaster("local[*]")
    sc = SparkContext(conf=conf)
    article_loc = sys.argv[1]
    featurefile = sc.textFile(sys.argv[2])
    features = featurefile.collect()
    dic = collections.Counter()
    j = 1

    for feature in features:
        dic[feature] = j
        j += 1

    extract_feature(sc, article_loc, dic, featurefile)