# from pyspark.sql import SparkSession
# spark = SparkSession.builder.master('local').appName("test").getOrCreate()
#
# df = spark.read.format('csv'). \
#     option('sep', '::'). \
#     load(r"C:\Users\Dell\Desktop\GFK Assesment Project\MovieLens_Project\movieLens_data_pipeline\src\main\python\staging\dimension\movies.dat")
#
# df.show(10)

import os
# print(os.listdir(r"C:\Users\Dell\Desktop\GFK Assesment Project\MovieLens_Project\movieLens_data_pipeline\src\main\python\output\pycharm_report"))
for i in os.listdir():
    print(i)

