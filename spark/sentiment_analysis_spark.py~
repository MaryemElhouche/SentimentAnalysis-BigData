# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf

# Créer SparkContext
conf = SparkConf().setAppName("Sentiment Analysis PySpark")
sc = SparkContext(conf=conf)

# Chemin HDFS
input_file = "/user/cloudera/SentimentAnalysis/data/sentimentsData_clean.csv"

#  Lire le CSV en RDD
lines = sc.textFile(input_file)

# Ignorer l'en-tête
header = lines.first()
data = lines.filter(lambda row: row != header)

# Extraire la colonne target (première colonne)
sentiments = data.map(lambda line: line.split(",")[0])

# Comptage
counts = sentiments.map(lambda s: (s, 1)).reduceByKey(lambda a, b: a + b)

# Afficher les résultats
for sentiment, count in counts.collect():
    print(sentiment, count)

# Stop SparkContext
sc.stop()

