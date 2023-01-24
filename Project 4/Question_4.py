# Databricks notebook source
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
spark = SparkSession(sc)

business = sc.textFile("/FileStore/tables/business.csv").map(lambda line: line.split("::")).toDF(["business_id","full_address","categories"]).distinct()
review = sc.textFile("/FileStore/tables/review.csv").map(lambda line: line.split("::")).toDF(["review_id","user_id","business_id","stars"]).distinct()

number_of_rankings = review.groupBy('business_id').count()
answer = number_of_rankings.join(business, "business_id").sort('count',ascending=False)
answer = answer.rdd.map(list).map(lambda x: [x[0]]+[x[2]]+[x[3]]+[x[1]])
formatted_rdd = sc.parallelize([answer.take(10)])
formatted_rdd.coalesce(1).saveAsTextFile("/FileStore/my-files/Q4out")
