import nltk


from pyspark import SparkContext, SparkConf
from operator import add
import string

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import *

import sys

APP_NAME = "Hello world"


def main(sc, text_file, file_name):

    textRDD = sc.textFile(text_file)
    stemmer = PorterStemmer()
    stop_words = stopwords.words('english')
    custom_stop_words = ["could", "one", "ms", "mr", "follow", "said", "has", "was", "would", "this", "hi", "like", "also", "say", "take"]
    stop_words = stop_words + custom_stop_words
    punctuation = string.punctuation

    words = textRDD.flatMap(lambda s: word_tokenize(s.lower()))
    stems = words.map(lambda w: stemmer.stem(w))\
        .filter(lambda p: p not in punctuation)\
        .filter(lambda p: re.sub(r'[^\x00-\x7f]', r'', p))\
        .filter(lambda s: s not in stop_words)\
        .filter(lambda a: a.isalpha() == True) \
        .map(lambda x: (x.encode('ascii', 'ignore'), 1))
    wordcount = stems.reduceByKey(add).collect()

    for (word, count) in wordcount:
        print("%s\t%d" % (word, count))

    orig_stdout = sys.stdout
    f = open(file_name+'.txt', 'w')
    sys.stdout = f

    for (word, count) in wordcount:
        print("%s\t%d" % (word, count))

    sys.stdout = orig_stdout
    f.close()

if __name__ == "__main__":
    conf = SparkConf().setAppName(APP_NAME).setMaster("local[*]")
    sc = SparkContext(conf=conf)
    text_file = sys.argv[1]
    file_name = sys.argv[2]
    main(sc, text_file, file_name)


