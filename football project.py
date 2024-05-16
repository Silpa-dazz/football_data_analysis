
# Q)create a data frame and load data to data frame print column names?

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('demo').getOrCreate()
uefa=spark.read.csv('hdfs://localhost:9000/sparkproject1/UEFAChampionsLeague2004-2021.csv',header=True,inferSchema=True)
uefa.show()
print('column names are:')
for i in uefa.columns:
    print(i)

# Q)to prnt no of columns?
print('no of columns are',len(uefa.columns))

# Q)prnt no of rows ?

print('no of rows are',uefa.count())

# Q)analysis: draw a graph of away team and home team goal scorinhg in each year of quaterfinal ,semifinal and final.(plot it as 2 graph) ?

from pyspark.sql import functions as f

newuefa=uefa.withColumn("date",f.from_unixtime(f.unix_timestamp(uefa.date),"yyyy-MM-dd"))
newuefa.show()
newuefa.printSchema()

from pyspark.sql.functions import udf
from pyspark.sql.functions import split

#every year home team other teams goals
def yeargenerator(x):
    li=x.split('-')
    return li[0]

myfn=udf(yeargenerator)
out=newuefa.withColumn('year',myfn(newuefa['date']))
out.show()



flt_out=out.filter((out['round']=='round : quarterfinals') | (out['round']=='round : semifinal')| (out['round']=='round : final'))
flt_out.show()

new=flt_out.select('homescore','awayscore','round','year')
new.show(n=50)
new.printSchema()


def myremove(value):
    return value[0]

newfn=udf(myremove)
one=new.withColumn('home_score',newfn(new['homescore']))
result=one.withColumn('away_score',newfn(one['awayscore']))

final= result.drop('homescore','awayscore')
final.show(n=50)
final.printSchema()

from pyspark.sql.types import IntegerType
newfd=final.withColumn('hmscore',final['home_score'].cast(IntegerType()))
finaldf=newfd.withColumn('awscore',newfd['away_score'].cast(IntegerType()))
finaldf=finaldf.drop('home_score','away_score')
finaldf.show()
finaldf.printSchema()
#
import pyspark.sql.functions as f
grp=finaldf.groupBy('year').agg(f.sum(finaldf['hmscore']).alias('totalhomegoals'),
                                f.sum('awscore').alias('totalawaygoals'))
grp.show()

grp=grp.orderBy('year')
import pandas as pd
df=grp.toPandas()

import matplotlib.pyplot as plt
plt.plot(df['year'],df['totalhomegoals'])
plt.plot(df['year'],df['totalawaygoals'])
plt.show()



# Q)analysis 2 -teams that most appeard in quarterfinal,semifinal and final ?

uefa=spark.read.csv('hdfs://localhost:9000/sparkproject1/UEFAChampionsLeague2004-2021.csv',header=True,inferSchema=True)
uefa.show()

newuefa=uefa.filter((uefa['round']=='round : quarterfinals')
                   | (uefa['round']=='round : semifinal')| (uefa['round']=='round : final'))
newuefa.show()


qf=uefa.filter(uefa['round']== 'round : quarterfinals')
sf=uefa.filter(uefa['round']== 'round : semifinals')
fi=uefa.filter(uefa['round']== 'round : final')

qf.show()
sf.show()
fi.show()

qf=qf.select('sl','homeTeam','round','date')
sf=sf.select('sl','homeTeam','round','date')
fi=fi.select('sl','homeTeam','round','date')

qf.show()
sf.show()
fi.show()

import pyspark.sql.functions as f

li=[qf,sf,fi]
for i in li:
    out1=i.groupBy('homeTeam').agg(f.count('Sl').alias('no_of_participation'))
    out2=out1.orderBy('no_of_participation',ascending=True)
    out2.show()

    maxvalue=out2.select(f.max(out2.no_of_participation))
    print(maxvalue.collect()[0])
    print('--------------------------')
