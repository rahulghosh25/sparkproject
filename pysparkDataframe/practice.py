from audioop import avg
from gc import collect
from itertools import count
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\data.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df.filter((df.brand=="adidas") & (df.amount==3)).show()
#df.show()
#df.groupBy("brand","type").sum("amount").show()
#df.select(df.amount,df.price).orderBy(df.amount.desc(),df.price).show()
#df.select("brand","type").filter(df.amount==10).orderBy(df.type.desc()).show()
df.select(avg("price")).show()