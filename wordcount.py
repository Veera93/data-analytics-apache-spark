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

APP_NAME = "Hello world"


def word_count(sc, base_location, folder_names):

    features = set([])

    for folder_name in folder_names:
        textRDD = sc.textFile(base_location+folder_name)
        stemmer = PorterStemmer()
        stop_words = stopwords.words('english')
        custom_stop_words = ["could", "one", "ms", "mr", "follow", "said", "ha", "wa", "would", "thi", "hi", "like", "also", "say", "take"]
        stop_words = stop_words + custom_stop_words
        punctuation = string.punctuation

        words = textRDD.flatMap(lambda s: word_tokenize(s.lower()))
        #words = textRDD.flatMap(lambda s: RegexpTokenizer('\w+').tokenize(s.lower()))

        stems = words.map(lambda w: stemmer.stem(w))\
            .filter(lambda p: p not in punctuation)\
            .filter(lambda p: re.sub(r'[^\x00-\x7f]', r'', p))\
            .filter(lambda s: s not in stop_words)\
            .filter(lambda a: a.isalpha() == True) \
            .map(lambda x: (x.encode('ascii', 'ignore'), 1))
        wordcount = stems.reduceByKey(add).sortBy(lambda a: a[1], ascending=False).take(40)
        for (word, count) in wordcount:
            features.add(word)
            print("%s\t%d" % (word, count))

        orig_stdout = sys.stdout
        f = open(folder_name+'.txt', 'w')
        sys.stdout = f

        for (word, count) in wordcount:
            print("%s\t%d" % (word, count))

        sys.stdout = orig_stdout
        f.close()
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
    #print("Outside word_count")
    #features = ["inning", "coach", "help", "execut", "becaus", "trade", "veri", "world", "go", "onli", "thank", "ela", "busi", "treatment", "uma", "tax", "senat", "hit", "get", "food", "financi", "de", "da", "game", "facebook", "know", "new", "report", "amp", "republican", "investor", "day", "bank", "wsj", "democrat", "secretari", "team", "dodger", "manag", "em", "right", "deal", "back", "rate", "expect", "year", "patient", "lead", "research", "state", "health", "got", "accord", "run", "million", "que", "let", "come", "care", "last", "drug", "thing", "american", "presid", "mani", "studi", "com", "think", "first", "clinton", "cancer", "win", "three", "governor", "market", "sander", "use", "support", "question", "two", "start", "wood", "firm", "regul", "para", "peopl", "fund", "yard", "compani", "look", "work", "second", "us", "um", "trump", "pass", "share", "player", "want", "home", "need", "end", "doctor", "make", "medic", "percent", "nyt", "field", "season", "stock", "play", "ball", "may", "befor", "plan", "bloomberg", "countri", "america", "applaus", "billion", "e", "invest", "well", "china", "time", "talk", "left"]
    print("-----> "+str(len(features)))
    dic = dict()

    i = 1
    for feature in features:
        dic[feature] = i
        i = i + 1

    extract_feature(sc, base_location, folder_names, dic)

    # orig_stdout = sys.stdout
    # f = open('input_vector.txt', 'w')
    # sys.stdout = f
    # random.shuffle(matrix)
    # for i in matrix:
    #     print(i)
    #
    # sys.stdout = orig_stdout




