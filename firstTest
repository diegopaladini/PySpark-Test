import os
import sys

os.environ['SPARK_HOME']='F:\softwares\spark-1.6.0-bin-hadoop2.6'
sys.path.append('F:\softwares\spark-1.6.0-bin-hadoop2.6\python')

from pyspark import SparkContext, SparkConf

sc = SparkContext( 'local[*]', 'pyspark')
db = sc.textFile('C:\\Users\\onlyc\\Desktop\\football.csv')
    

def cleaning(fields):
    
    fmod = []
    for field in fields:
        field.strip()
        field.replace("-", "_")
        field.replace(" ", "_")
        fmod.append(field + "TEST")
    return fmod
    
    
    
dirtyRows = db.map(lambda line: line.split(","))
sample = dirtyRows.sample("false", 0.1)
sample.count()
sample.take(10)
cleanRows = dirtyRows.map(lambda line: cleaning(line))
header = cleanRows.take(2)
header
sc.stop()

