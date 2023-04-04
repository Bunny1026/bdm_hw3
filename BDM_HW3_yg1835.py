import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import csv
from pyspark import SparkContext
import sys

if __name__=='__main__':
    sc = pyspark.SparkContext.getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    sc.textFile(sys.argv[1] if len(sys.argv)>1 else '/shared/CUSP-GX-6002/data/complaints.csv'use_unicode=True).cache() \
    header = C.first().split(',')    
    D = C.mapPartitionsWithIndex(extractFeatures)
    
    def extractFeatures(partId, rows):
    if partId == 0:
        next(rows)
    reader = csv.reader(rows)
    for row in reader:
        if len(row) == 18 and len(row[0])==10:
            product, date, company,comp_id = row[1],row[0],row[7],row[17]
            year = date.strip().split("-")[0]
            yield(product, year, company,comp_id,1)
    D11 = D.map(lambda x: ((x[0].strip().lower(),x[1],x[2].strip().lower()),1))\
      .reduceByKey(lambda x,y: x+y)\
      .filter(lambda x: x[1]>0)\
      .map(lambda x: ((x[0][0],x[0][1]),1))\
      .reduceByKey(lambda x,y: x+y)\
      .sortBy(lambda x: (x[0][0],x[0][1]))

    D2 = D.map(lambda x: ((x[0].strip().lower(),x[1]),1))\
      .reduceByKey(lambda x,y: x+y)\
      .sortBy(lambda x: (x[0][0],x[0][1]))\
      .join(D11)

    # get the total number of complaints received per product, per year
    D3 = D.map(lambda x: ((x[0].strip().lower(),x[1]),1))\
      .reduceByKey(lambda x,y: x+y)\
      .sortBy(lambda x: (x[0][0],x[0][1]))

    # get the percentage
    D4 = D.map(lambda x: ((x[0].strip().lower(),x[1],x[2].strip().lower()),1))\
      .reduceByKey(lambda x,y: x+y)\
      .map(lambda x: ((x[0][0],x[0][1]),(x[0][2],x[1])))\
      .groupByKey().mapValues(lambda x: max(x,key=lambda y:y[1]))\
      .join(D3)\
      .sortBy(lambda x: (x[0][0],x[0][1]))\
      .map(lambda x: ((x[0][0],x[0][1]),round(x[1][0][1]/x[1][1]*100)))
      
    outputTask1 = D4.join(D2)\
                .map(lambda x: (x[0][0],x[0][1],x[1][1][0],x[1][1][1],x[1][0]))\
                .sortBy(lambda x: (x[0],x[1]))\
                .map(lambda x: ','.join(map(str, x)))\
                .saveAsTextFile(sys.argv[2] if len(sys.argv)>2 else 'output')

            
