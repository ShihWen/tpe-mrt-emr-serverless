from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from datetime import datetime

spark = SparkSession.builder.appName("mrt-spark").getOrCreate()
exe_time = datetime.now().strftime("%Y%m%d_%H-%M-%S")

# Load and transform data
data_path = 's3n://tpe-mrt-data/hourly-traffic/mrt_q_20*.parquet'
df = spark.read.parquet(data_path)
df = df.select(func.col("dt").cast(DateType())
               , func.col("hour")
               , func.col("entrance")
               , func.col("exit")
               , func.col("traffic"))

# Generate traffic date by station
daily_entrance = df.groupBy(func.col('dt'), func.col('entrance'), func.col('hour'))\
                    .agg( func.sum(func.col('traffic')).alias('entrance_traffic') )

daily_exit = df.groupBy(func.col('dt'), func.col('exit'), func.col('hour'))\
               .agg( func.sum(func.col('traffic')).alias('exit_traffic') )


daily_traffic = daily_entrance.alias('in')\
                              .join(\
                                    daily_exit.alias('out')
                                    , (func.col('in.dt')==func.col('out.dt'))\
                                    & (func.col('in.entrance')==func.col('out.exit'))
                                    & (func.col('in.hour')==func.col('out.hour'))
                                   )\
                              .select(\
                                      func.col("in.dt").alias("date")
                                      , func.col("in.hour")
                                      , func.col("in.entrance").alias("station")
                                      , func.col("in.entrance_traffic").alias("in_traffic")
                                      , func.col("out.exit_traffic").alias("out_traffic")
                                     )

daily_traffic = daily_traffic.withColumn('dow', func.dayofweek(daily_traffic.date))\
                             .withColumn('ttl_traffic', func.col('in_traffic')+func.col('out_traffic')) 
daily_traffic = daily_traffic.select("date","dow", "hour","station","in_traffic","out_traffic","ttl_traffic")

#### OUTPUTs ####

# 1. Monthly traffic
daily_traffic.withColumn("year", func.year(func.col("date")))\
             .withColumn("month", func.month(func.col("date")))\
             .withColumn( "last_day", func.dayofmonth( func.last_day(func.col("date"))) )\
             .groupBy( func.concat_ws('-', func.col("year"), func.col("month")).alias("yr-month")
                       , func.col("last_day"))\
             .agg(func.sum(func.col("ttl_traffic")).alias("sum")).orderBy(func.col("yr-month"))\
             .write.option("header",True).csv(f's3n://tpe-mrt-data/output/mthly-01-ttl-traffic-{exe_time}')


daily_traffic.withColumn("year", func.year(func.col("date")))\
             .withColumn("month", func.month(func.col("date")))\
             .withColumn( "last_day", func.dayofmonth( func.last_day(func.col("date"))) )\
             .groupBy( func.concat_ws('-', func.col("year"), func.col("month")).alias("yr-month")
                       , func.col("last_day")
                       , func.col("station"))\
             .agg(func.sum(func.col("ttl_traffic")).alias("sum"))\
             .orderBy(func.col("yr-month"), func.col("sum").desc())\
             .write.option("header",True).csv(f's3n://tpe-mrt-data/output/mthly-02-station-traffic-{exe_time}')


station_path = 's3n://tpe-mrt-data/job-scripts/mrt_station.csv'
df_station = spark.read.option("header", "true").option("inferSchema", "true").csv(station_path)\
                  .select(func.col('StationID').alias('station_id'), func.col('station_join_key').alias('station'))\
                  .distinct()\
                  .withColumn('line', func.regexp_extract('station_id','[A-Z]+',0))

daily_traffic_id = daily_traffic.alias('a').join(\
                                              df_station.alias('b')
                                              , (func.col('a.station')==func.col('b.station'))
                                             )\
                                        .withColumn('weekOfYr', func.weekofyear(func.col("date")))\
                                        .select(func.col('a.date')
                                                , func.col('a.dow')
                                                , func.col('weekOfYr') 
                                                , func.col('a.hour')
                                                , func.col('a.station')
                                                , func.col('b.station_id')
                                                , func.col('b.line')
                                                , func.col('a.in_traffic')
                                                , func.col('a.out_traffic')
                                                , func.col('a.ttl_traffic'))

daily_traffic_id.withColumn("year", func.year(func.col("date")))\
                .withColumn("month", func.month(func.col("date")))\
                .withColumn( "last_day", func.dayofmonth( func.last_day(func.col("date"))) )\
                .groupBy( func.concat_ws('-', func.col("year"), func.col("month")).alias("yr-month")
                          , func.col("last_day")
                          , func.col("line"))\
                .agg(func.sum(func.col("ttl_traffic")).alias("sum"))\
                .orderBy(func.col("yr-month"), func.col("line"))\
                .write.option("header",True).csv(f's3n://tpe-mrt-data/output/mthly-03-line-traffic-{exe_time}')


spark.stop()