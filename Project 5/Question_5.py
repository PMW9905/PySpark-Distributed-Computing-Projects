# Databricks notebook source
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
spark = SparkSession(sc)

review = sc.textFile("/FileStore/tables/review.csv").map(lambda line: line.split("::")).toDF(["review_id","user_id","business_id","stars"]).distinct()
user = sc.textFile("/FileStore/tables/user.csv").map(lambda line: line.split("::")).toDF(["user_id","name","url"]).distinct()

number_of_reviews = review.groupBy('user_id').count()
total_number_of_reviewers = review.count()
answer = number_of_reviews.join(user, "user_id").sort('count',ascending=True)
answer = answer.rdd.map(list).map(lambda x: str(x[2])+"    "+str(x[1]/total_number_of_reviewers)+"%")
formatted_rdd = sc.parallelize([answer.take(10)])
formatted_rdd.coalesce(1).saveAsTextFile("/FileStore/my-files/Q5out")
