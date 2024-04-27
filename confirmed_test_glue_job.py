import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, coalesce, to_date, expr, date_format, sum
import boto3
from pyspark.sql import DataFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session.builder.appName("example").config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()
s3_client = boto3.client('s3')

def readCSV(spark, file):
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)
    df.show()
    cols = [x.name.lower() for x in df.schema.fields]
    
    non_date_cols = [y for y in cols if y[0] >= 'a' and y[0] <= 'z']
    date_cols = [y for y in cols if y[0] >= '0' and y[0] <= '9']
    
    date_cols_val = ",".join(date_cols)
    
    stack_exp = ",".join([f"'{x}',`{x}`" for x in date_cols])
    stack_exp = f"stack({len(date_cols)},{stack_exp}) as (dates,value)"
    
    df.createOrReplaceTempView("df")
    
    df_final = spark.sql(f"""select coalesce(`province/state`,`country/region`) as `province/state`,
                             `country/region`,lat,long,{stack_exp}
                             from df""")
    df_final.createOrReplaceTempView("df_final")
    
    return spark.sql("""select `province/state`,`country/region`,lat,long,
                         to_date(dates,'MM/dd/yy') as dates,
                         cast(value as long) as value
                         from df_final""")

confirmedDf=readCSV(spark, "s3://projectpro-covid19-test-data/covid19/confirmed/time_series_covid19_confirmed_global.csv")
deathDf=readCSV(spark,"s3a://projectpro-covid19-test-data/covid19/deaths/time_series_covid19_deaths_global.csv")
recoveredDf=readCSV(spark,"s3a://projectpro-covid19-test-data/covid19/recovered/time_series_covid19_recovered_global.csv")

outputPath = "s3a://projectpro-covid19-test-data/covid19/processedData"

def get_bucket_name_and_key(s3_path):
    bucket_name, key = s3_path.replace("s3a://", "").split("/", 1)
    file_key = key[0:]
    return bucket_name, file_key

def get_s3_object_count(file_path):
    bucket_name, key = get_bucket_name_and_key(file_path)
    key_count = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key)['KeyCount']
    return key_count

def import_df(spark, confirmed_df, death_df, recovered_df, output_path):
    confirmed_df.createOrReplaceTempView("confirmedDf")
    death_df.createOrReplaceTempView("deathDf")
    recovered_df.createOrReplaceTempView("recoveredDf")
    
    spark.sql("""select cast(a.`province/state` as string) as `province/state`, cast(a.`country/region` as string) as `country/region`, cast(a.lat as string) as lat, cast(a.long as string) as long, cast(a.dates as date) as dates, cast(coalesce(a.value,0) as int) as confirmed,
                cast(coalesce(b.value,0) as int) as death, cast(coalesce(c.value,0) as int)as recovered, cast(date_format(a.dates, 'MMM-yy') as string) as month_year,
                cast(sum(coalesce(a.value,0)) over(partition by a.`province/state`, a.`country/region` order by a.dates rows between unbounded preceding and current row) as int) as confirmed_daily,
                cast(sum(coalesce(b.value,0)) over(partition by a.`province/state`, a.`country/region` order by a.dates rows between unbounded preceding and current row) as int) as deaths_daily,
                cast(sum(coalesce(c.value,0)) over(partition by a.`province/state`, a.`country/region` order by a.dates rows between unbounded preceding and current row) as int) as recovered_daily,current_date() as load_dt,current_timestamp() as load_dtm
                from confirmedDf a
                left join deathDf b
                on a.`province/state` = b.`province/state` and a.`country/region` = b.`country/region` and a.dates = b.dates
                left join recoveredDf c
                on a.`province/state` = c.`province/state` and a.`country/region` = c.`country/region` and a.dates = c.dates
                """).createOrReplaceTempView("sourceDf")

    obj_count = get_s3_object_count(output_path)
    if obj_count == 0:
        return spark.sql("""select * from sourceDf""")
    else:
        spark.read.parquet(output_path).createOrReplaceTempView("outputDf")
        return spark.sql("""select a.`province/state`, a.`country/region`, a.lat, a.long, a.dates,
                            a.confirmed, a.death, a.recovered, a.month_year, a.confirmed_daily,
                            a.deaths_daily, a.recovered_daily, a.load_dt,a.load_dtm
                            from sourceDf a
                            left join outputDf b
                            on a.`province/state` = b.`province/state` and a.`country/region` = b.`country/region` and a.dates = b.dates
                            where b.`province/state` is null and b.`country/region` is null and b.dates is null
                            """)

inputDf=import_df(spark, confirmedDf, deathDf, recoveredDf, outputPath)

def exportDf(spark, inputDf, outputPath) -> None:
    inputDf.write.mode('append').parquet(outputPath)
    
exportDf(spark,inputDf,outputPath)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()