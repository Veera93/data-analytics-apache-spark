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

    for text_file in folder_names:
        textRDD = sc.textFile(base_location+text_file)
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
        f = open(text_file+'.txt', 'w')
        sys.stdout = f

        for (word, count) in wordcount:
            print("%s\t%d" % (word, count))

        sys.stdout = orig_stdout
        f.close()
    return features

def extract_feature(sc, base_location, folder_names, features):
    doc_id = -1
    input_matrix = []
    for folder in folder_names:
        doc_id = doc_id + 1
        location = os.getcwd()+"/"+base_location+folder

        for file in os.listdir(location):
            if file.endswith(".txt"):
                text_file = os.path.join(location+"/", file)
                textRDD = sc.textFile(text_file)
                stemmer = PorterStemmer()
                stop_words = stopwords.words('english')
                custom_stop_words = ["could", "one", "ms", "mr", "follow", "said", "ha", "wa", "would", "thi", "hi",
                                     "like", "also", "say", "take"]
                stop_words = stop_words + custom_stop_words
                punctuation = string.punctuation

                words = textRDD.flatMap(lambda s: word_tokenize(s.lower()))
                #print("-------------------------->" + str(words.count()))
                #print("-------------------------->" + file)
                total_words = words.count()
                stems = words.map(lambda w: stemmer.stem(w)) \
                    .filter(lambda p: p not in punctuation) \
                    .filter(lambda p: re.sub(r'[^\x00-\x7f]', r'', p)) \
                    .filter(lambda s: s not in stop_words) \
                    .filter(lambda a: a.isalpha() == True) \
                    .filter(lambda z: z in features) \
                    .map(lambda x: (x.encode('ascii', 'ignore'), 1))
                wordcount = stems.reduceByKey(add).collect()
                count = collections.Counter()
                for (key, value) in wordcount:
                    count[key] = value
                input_vector = []
                input_vector.append(str(doc_id))
                input_vector.append(" ")
                i = 1
                for feature in features:
                    input_vector.append(str(i))
                    input_vector.append(":")
                    if(count[feature] > 0):
                        input_vector.append(str(count[feature]))
                    else:
                        input_vector.append(str(0))
                    input_vector.append(" ")
                    i = i+1
                input_matrix.append(''.join(input_vector))
    return input_matrix




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

    matrix = extract_feature(sc, base_location, folder_names, features)

    orig_stdout = sys.stdout
    f = open('input_vector.txt', 'w')
    sys.stdout = f
    random.shuffle(matrix)
    for i in matrix:
        print(i)

    sys.stdout = orig_stdout




