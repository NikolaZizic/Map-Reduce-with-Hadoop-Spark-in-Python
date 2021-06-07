# -*- coding: utf-8 -*-
"""
Created on Sun Jun  6 20:51:18 2021

@author: 850
"""

import findspark



findspark.init(("C:\Spark\spark-3.1.2-bin-hadoop3.2"))

from pyspark.sql.session import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = spark.sparkContext


def clean_str(line):
  line = line.lower().replace(".","").replace("?","")
  return line.split(" ")

book = sc.textFile("Sherlock.txt")
book = book.filter(lambda x : len(x)>0)
word_count = book.flatMap(clean_str).map(lambda w : (w,1)).reduceByKey(lambda a,b: a+b)
word_count.takeOrdered(50,key=lambda pair: -pair[1])







# 1. Editing the data : adding a year column and a word_count column 

import pandas as pd

df= pd.read_csv("abcnews-date-text.csv")

df.dtypes

df["word_count"] = df["headline_text"].str.split().str.len()
df["year"] = (df["publish_date"]).astype(str)
df['year'] = [x[:4] for x in df['year']]

df.to_csv("abcnews-date-text_edit.csv")

df.dtypes


# 2. Loading the edited data with spark

import findspark
from pyspark.sql.session import SparkSession

findspark.init(("C:\Spark\spark-3.1.2-bin-hadoop3.2"))

spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = spark.sparkContext

df2 = spark.read.format("csv").option("header", "true").load("abcnews-date-text_edit.csv")
# df.show()
# df.columns

rdd = df2.rdd

# 3. Applying map/reduce and arranging by occurrence

def word_count_year(x):
    year = x.year
    word_count = x.word_count
    return (year,word_count),1 
    

count_per_year = rdd.map(word_count_year)

cum_count_per_year = count_per_year.reduceByKey( lambda a,b : a+b)
cum_count_per_year.takeOrdered(50,key=lambda pair: -pair[1])


# 4. Basic bar plot visualizatioin 


an_freq_df = cum_count_per_year.toDF(["Words per title for each year","count"])
pdDF = an_freq_df.orderBy('Words per title for each year',ascending=True).toPandas()
pdDF.plot(x='Words per title for each year', y='count', kind='bar', color='red',rot=60,figsize=(200,50))
display()

