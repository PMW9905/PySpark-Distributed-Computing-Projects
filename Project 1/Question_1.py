# Databricks notebook source
def create_pair(line):
    pairs = []
    if len(line[1]) != 0:
        for friend in line[1]:
            if friend<line[0]:
                pairs.append( (friend+","+line[0],line[1]) )
            else:
                pairs.append( (line[0]+","+friend,line[1]) )
        return pairs
    return

lines_rdd = sc.textFile("/FileStore/tables/mutual.txt")
singles_rdd = lines_rdd.map(lambda line: line.split("\t")).map(lambda x: (x[0],x[1].split(",")))
pairs_rdd = singles_rdd.flatMap(create_pair).reduceByKey(lambda x,y: list(set(x).intersection(y))).filter(lambda x: len(x[1])!=0).map(lambda x: (x[0],len(x[1])))
formatted_pairs_rdd = pairs_rdd.map(lambda x: str(x[0])+"    "+str(x[1]))
formatted_pairs_rdd.coalesce(1).saveAsTextFile("/FileStore/my-files/Q1out")
