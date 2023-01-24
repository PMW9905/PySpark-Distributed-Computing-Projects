# Databricks notebook source
from itertools import chain
from pyspark.mllib.linalg import Matrices
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

import numpy as np

spark = SparkSession(sc)

def string_to_int(l):
    return [int(i) for i in l]
    

matrix1 = sc.textFile("/FileStore/tables/small_matrix1.txt").map(lambda x: x.split(" ")[:-1]).map(string_to_int).zipWithIndex().map(lambda x: IndexedRow(x[1],[x[1]]+x[0]))
matrix2 = sc.textFile("/FileStore/tables/small_matrix2.txt").map(lambda x: x.split(" ")[:-1]).map(string_to_int).map(lambda x: [0]+x)
row_of_one_then_zeros = sc.parallelize([[1]+[0]*100])
matrix2 = row_of_one_then_zeros + matrix2

index_row_matrix = IndexedRowMatrix(matrix1)
local_matrix2 = list(chain.from_iterable(matrix2.collect()))
dense_matrix = Matrices.dense(101,101,local_matrix2)

result = index_row_matrix.multiply(dense_matrix)
result.rows.coalesce(1).saveAsTextFile("/FileStore/my-files/Q6out")
