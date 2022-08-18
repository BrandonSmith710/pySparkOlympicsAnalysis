from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession \
        .builder \
        .appName("Spark_1") \
        .getOrCreate()

def get_dataframe(filename):
    df = spark.read.format('csv').options(header = 'true').load(filename)
    return df


df_winter = get_dataframe('winter.csv')
df_winter = df_winter.withColumn('Year', df_winter['Year'].cast(LongType()))
df_winter.createOrReplaceTempView('winter')



# 1) Which countries won the most Gold Medals each year?

df_winter.filter(col('Medal') == 'Gold') \
    .groupBy('Year', 'Country') \
    .agg(count('*').alias('Gold_Count')) \
    .select('*', rank().over(Window.partitionBy('Year') \
        .orderBy(desc('Gold_Count'))).alias('rank')) \
    .filter(col('rank') == 1) \
    .orderBy(desc('Year')) \
    .select('Year', 'Country', 'Gold_Count') \
    .show()

spark.sql('''with one as
		 (SELECT Year
		  , Country
		  , COUNT(*) as Gold_Count
		  , rank() OVER( PARTITION BY Year
	   	  		 ORDER BY COUNT(*) DESC ) as rank
 	          FROM winter
 	     	  WHERE Medal = "Gold"
	          GROUP BY Year, Country)
	     SELECT Year
	     , Country
	     , Gold_Count
	     FROM one
	     WHERE rank = 1
       	     ORDER BY Year DESC;''').show()


# 2) Which countries won the most Gold Medals in each sport for all years combined?

df_winter.filter(col('Medal') == 'Gold') \
    .groupBy('Sport', 'Country') \
    .agg(count('*').alias('Gold_Count')) \
    .select('*', rank().over(Window.partitionBy('Sport') \
        .orderBy(desc('Gold_Count'))).alias('rank')) \
    .filter(col('rank') == 1) \
    .orderBy('Sport') \
    .select('Sport', 'Country', 'Gold_Count') \
    .show()

spark.sql('''with one as
		 (SELECT Sport
		  , Country
	   	  , COUNT(*) as Gold_Count
	   	  , rank() OVER( PARTITION BY Sport
	   	  		 ORDER BY COUNT(*) DESC ) as rank
		  FROM winter
		  WHERE Medal = "Gold"
		  GROUP BY Sport, Country)
	     SELECT Sport
	     , Country
	     , Gold_Count
	     FROM one
	     WHERE rank = 1
	     ORDER BY Sport;''').show()


# 3) Which countries won the most silver medals in each sport each year?

df_winter.filter(col('Medal') == 'Silver') \
    .groupBy('Year', 'Sport', 'Country') \
    .agg(count('*').alias('Silver_Count')) \
    .select('*', rank().over(Window.partitionBy('Year', 'Sport') \
        .orderBy(desc('Silver_Count'))).alias('rank')) \
    .filter(col('rank') == 1) \
    .orderBy(desc('Year'), 'Sport') \
    .select('Year', 'Sport', 'Country', 'Silver_Count') \
    .show()

spark.sql('''with one as
		 (SELECT Year
		  , Sport
		  , Country
		  , COUNT(*) as Silver_Count
		  , rank() OVER( PARTITION BY Year, Sport
  				 ORDER BY COUNT(*) DESC ) as rank
		  FROM winter
		  WHERE Medal = "Silver"
		  GROUP BY Year, Sport, Country)
	     SELECT Year
	     , Sport
	     , Country
	     ,Silver_Count
	     FROM one
	     WHERE rank = 1
	     ORDER BY Year DESC, Sport;''').show()


# 4) How many events did each country compete in per olympic games on average?

df_winter.groupBy('Country', 'Year') \
    .agg(countDistinct('Event').alias('num_events')) \
    .select('Country', round(avg('num_events') \
        .over(Window.partitionBy('Country'))).cast(LongType()) \
	.alias('avg_num_events')).distinct() \
    .orderBy(desc('avg_num_events')).show()

spark.sql('''with one as
		 (SELECT Country
		  , Year
		  , COUNT( DISTINCT Event ) as num_events
		  FROM winter
		  GROUP BY Country, Year)
	     SELECT DISTINCT Country
	     , ROUND( CAST( AVG( num_events )
	         OVER( PARTITION BY Country ) as numeric) ) as avg_num_events
	     FROM one
             ORDER BY avg_num_events DESC;''').show()


# 5) Which athletes won the second most gold medals each year?

df_winter.filter(col('Medal') == 'Gold') \
	.groupBy('Year', 'Athlete') \
	.agg(count('*').alias('Gold_Count')) \
	.select('*', rank().over(Window.partitionBy('Year') \
	    .orderBy(desc('Gold_Count'))).alias('rank')) \
	.filter(col('rank') == 2) \
        .orderBy(desc('Year'), 'Athlete') \
	.select('Year', 'Athlete', 'Gold_Count') \
	.show()

spark.sql('''with one as
		 (SELECT Year
		  , Athlete
		  , COUNT(*) as Gold_Count
		  , rank() OVER( PARTITION BY Year
  				 ORDER BY COUNT(*) DESC ) as rank
		  FROM winter
		  WHERE Medal = "Gold"
		  GROUP BY Year, Athlete)
	     SELECT Year
    	     , Athlete
	     , Gold_Count
	     FROM one
	     WHERE rank = 2
	     ORDER BY Year DESC, Athlete;''').show()


# 6) Which athletes won the most gold medals for all years combined?

df_winter.filter(col('Medal') == 'Gold') \
    .groupBy('Athlete') \
    .agg(count('*').alias('Gold_Count')) \
    .orderBy(desc('Gold_Count')).show()

spark.sql('''SELECT Athlete, COUNT(*) as Gold_Count
	     FROM winter
	     WHERE Medal = "Gold"
  	     GROUP BY Athlete
	     ORDER BY Gold_Count DESC;''').show()


# 7) How many gold, silver and bronze medals did each country win each year?

df_winter.groupBy('Year', 'Medal', 'Country') \
    .agg(count('*').alias('Medal_Count')) \
    .orderBy(desc('Year'), 'Country',
 	when(col('Medal') == 'Gold', 1)
    	.when(col('Medal') == 'Silver', 2)
	.when(col('Medal') == 'Bronze', 3)) \
	.show()

spark.sql('''SELECT Year
	     , Medal
  	     , Country
	     , COUNT(*) as Medal_Count
	     FROM winter
             GROUP BY Year, Medal, Country
             ORDER BY Year DESC, Country, (CASE Medal
			       		   WHEN "Gold" THEN 1
			    		   WHEN "Silver" THEN 2
			         	   WHEN "Bronze" THEN 3
			         	   END);''').show()


# 8) What event(s) did USA win the most gold medals in at the 1972 games?

df_winter.filter((col('Year') == 1972) \
    & (col('Country') == 'USA') \
    & (col('Medal') == 'Gold')) \
    .groupBy('Event') \
    .agg(count('*').alias('Gold_Count')) \
    .orderBy(desc('Gold_Count')) \
    .show()

spark.sql('''SELECT Event
	     , COUNT(*) as Gold_Count
	     FROM winter
	     WHERE Year = 1972
	     AND Country = "USA"
	     AND Medal = "Gold"
	     GROUP BY Event
	     ORDER BY Gold_Count DESC;''').show()


# 9) Which city(s) has USA won the most gold medals in since 1955?

df_winter.filter((col('Country') == 'USA') \
    & (col('Medal') == 'Gold') \
    & (col('Year') >= 1955)) \
    .groupBy('City') \
    .agg(count('*').alias('Gold_Count')) \
    .orderBy(desc('Gold_Count')) \
    .show()

spark.sql('''SELECT City
	     , COUNT(*) as Gold_Count
	     FROM winter
	     WHERE Country = "USA"
	     AND Medal = "Gold"
             AND Year >= 1955
	     GROUP BY City
	     ORDER BY Gold_Count DESC;''').show()


# 10) Which year(s) had the highest number of female competitors?

df_winter.filter(col('Gender') == 'Women') \
    .groupBy('Year') \
    .agg(countDistinct('Athlete').alias('num_females')) \
    .orderBy(desc('num_females')) \
    .show()

spark.sql('''SELECT Year
	     , COUNT(DISTINCT Athlete) as num_females
             FROM winter WHERE Gender = 'Women'
 	     GROUP BY Year
 	     ORDER BY num_females DESC;''').show()


# 11) What sport does each country have the most gold medals in for all years combined?

df_winter.filter(col('Medal') == 'Gold') \
    .groupBy('Country', 'Sport') \
    .agg(count('*').alias('Gold_Count')) \
    .select('*', rank().over(Window.partitionBy('Country') \
        .orderBy(desc('Gold_Count'))).alias('rank')) \
    .filter(col('rank') == 1) \
    .orderBy('Country') \
    .select('Country', 'Sport', 'Gold_Count') \
    .show()

spark.sql('''with one as
		 (SELECT Country
		  , Sport
		  , COUNT(*) as Gold_Count
		  , rank() OVER ( PARTITION BY Country
				  ORDER BY COUNT(*) DESC ) as rank
		  FROM winter
		  WHERE Medal = "Gold"
		  GROUP BY Country, Sport)
	     SELECT Country
	     , Sport
	     , Gold_Count
	     FROM one
	     WHERE rank = 1
	     ORDER BY Country;''').show()
