# Databricks notebook source
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
spark = SparkSession(sc)

business = sc.textFile("/FileStore/tables/business.csv").map(lambda line: line.split("::")).toDF(["business_id","full_address","categories"]).distinct()
review = sc.textFile("/FileStore/tables/review.csv").map(lambda line: line.split("::")).toDF(["review_id","user_id","business_id","stars"]).distinct()
user = sc.textFile("/FileStore/tables/user.csv").map(lambda line: line.split("::")).toDF(["user_id","name","url"]).distinct()

business_stanford = business.filter(business.full_address.contains("Stanford"))
review_stanford = business_stanford.join(review, "business_id")
reviewer_stanford = review_stanford.join(user, "user_id")

answer = reviewer_stanford.select("name","stars")
answer.rdd.coalesce(1).saveAsTextFile("/FileStore/my-files/Q3out")
