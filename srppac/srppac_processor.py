from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
import argparse
from pathlib import Path

CH2NS = 0.0009765625 # AMANEQ HRTDC time unit to ns
TIME_RANGE_NS = (-76, 1000)
HIT_SIZE_LIMIT = 7

def decode_srppac_amaneq_rpa(spark: SparkSession, df: DataFrame) -> DataFrame:
    # Filter SRPPAC strip data and decode
    df_src = df.filter("femType==7 and femId==614").select("data").withColumn("decoded",F.expr("decode_hrtdc_unpaired_segdata(data)"))
    df_src = df_src.select("decoded.*").select("hbf.*","data").select("hbfNumber","data").filter("array_size(decoded.data)>0")
    df_src = df_src.withColumn("ex",F.explode("data")).select("hbfNumber","ex.*").withColumnRenamed("tot","edge")
    df_src = df_src.withColumn("tcal", F.expr(f"(time + rand())*{CH2NS}")).drop("time")

    # Filter SRPPAC anode data and decode
    df_sra = df.filter("femType==5 and femId==615").select("data").withColumn("decoded",F.expr("decode_hrtdc_segdata(data)"))
    df_sra = df_sra.select("decoded.*").select("hbf.*","data").select("hbfNumber","data").filter("array_size(decoded.data)>0")
    df_sra = df_sra.withColumn("ex",F.explode("data")).select("hbfNumber","ex.*").filter("ch==3")
    df_sra = df_sra.withColumn("tcal", F.expr(f"(time + rand())*{CH2NS}")).drop("time")
    df_sra = df_sra.groupBy("hbfNumber").agg(F.min("tcal").alias("tcal_a")) # select fastest anode hit

    # Join strip and anode data
    df_sr = df_src.join(df_sra, on=["hbfNumber"])
    df_sr = df_sr.withColumn("tcal_c", F.expr("tcal-tcal_a")).drop("tcal").drop("tcal_a")
    df_sr = df_sr.filter(f"tcal_c > {TIME_RANGE_NS[0]} and tcal_c < {TIME_RANGE_NS[1]}")
    dfL = df_sr.filter("edge=0").select("hbfNumber","tcal_c","ch").withColumnRenamed("tcal_c","timingL")
    dfT = df_sr.filter("edge=1").select("hbfNumber","tcal_c","ch").withColumnRenamed("tcal_c","timingT")
    df_sr = dfL.join(dfT, on=["hbfNumber","ch"])
    df_sr = df_sr.withColumn("charge", F.expr("timingT - timingL"))

    # Map strip channel to rpa id for X
    df_xmap = spark.read.csv('map/srx_rpa_map.csv', inferSchema = True, header = True).withColumn("id", F.col("id").cast("int"))
    df_x = df_sr.join(df_xmap, on = ["ch"]).drop("ch")

    df_x = df_x.orderBy(F.col("charge").desc()).groupBy("hbfNumber").agg(F.collect_list("id").alias("id"), F.collect_list("timingL").alias("timing"), F.collect_list("charge").alias("charge"))
    df_x = df_x.withColumn("size", F.expr("size(id)"))
    df_x = df_x.withColumn("timing0",F.expr(f"try_element_at(timing, 1)")) \
             .withColumn("charge0",F.expr(f"try_element_at(charge, 1)")) \
             .withColumn("charge1",F.expr(f"try_element_at(charge, 2)")) \
             .withColumn("charge2",F.expr(f"try_element_at(charge, 3)")) \
             .withColumn("id0",F.expr(f"try_element_at(id, 1)")) \
             .withColumn("id1",F.expr(f"try_element_at(id, 2)")) \
             .withColumn("id2",F.expr(f"try_element_at(id, 3)"))

    # Map strip channel to rpa id for Y
    df_ymap = spark.read.csv('map/sry_rpa_map.csv', inferSchema = True, header = True).withColumn("id", F.col("id").cast("int"))
    df_y = df_sr.join(df_ymap, on = ["ch"]).drop("ch")

    df_y = df_y.orderBy(F.col("charge").desc()).groupBy("hbfNumber").agg(F.collect_list("id").alias("id"), F.collect_list("timingL").alias("timing"), F.collect_list("charge").alias("charge"))
    df_y = df_y.withColumn("size", F.expr("size(id)"))
    df_y = df_y.withColumn("timing0",F.expr(f"try_element_at(timing, 1)")) \
             .withColumn("charge0",F.expr(f"try_element_at(charge, 1)")) \
             .withColumn("charge1",F.expr(f"try_element_at(charge, 2)")) \
             .withColumn("charge2",F.expr(f"try_element_at(charge, 3)")) \
             .withColumn("id0",F.expr(f"try_element_at(id, 1)")) \
             .withColumn("id1",F.expr(f"try_element_at(id, 2)")) \
             .withColumn("id2",F.expr(f"try_element_at(id, 3)"))

    # Add suffix _y to all columns except hbfNumber
    df_x = df_x.select([F.col(col).alias(f"{col}_x") if col != "hbfNumber" else F.col(col) for col in df_x.columns])
    df_y = df_y.select([F.col(col).alias(f"{col}_y") if col != "hbfNumber" else F.col(col) for col in df_y.columns])
    rdf = df_x.join(df_y, on=["hbfNumber"])
    return rdf

def calib_srppac_dqdx(spark: SparkSession, df: DataFrame) -> DataFrame:
    # Calibrate SRPPAC data
    # Number of hit strips should be between 2 and HIT_SIZE_LIMIT-1
    df = df.filter(f"size_x<{HIT_SIZE_LIMIT} and size_y<{HIT_SIZE_LIMIT}")
    df = df.filter("size_x>1 AND size_y>1")
    # Charge0 and Charge1 strips should be adjacent
    df = df.filter("ABS(id0_x - id1_x) = 1 AND ABS(id0_y - id1_y) = 1")
    # Define q0q1 for calibration parameter
    df = df.withColumn("q0q1_x", F.expr("(charge0_x - charge1_x)/(charge0_x + charge1_x)"))
    df = df.withColumn("q0q1_y", F.expr("(charge0_y - charge1_y)/(charge0_y + charge1_y)"))

    rdf = df.select("hbfNumber", "q0q1_x", "q0q1_y")
    return rdf

def calib_srppac_dqdxlr(spark: SparkSession, df: DataFrame) -> DataFrame:
    # Calibrate SRPPAC data

    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='process srppac data from asagi')

    parser.add_argument('input_file', help='input file')
    parser.add_argument('--output-file', help='output file')
    parser.add_argument('--output-strip-data', action='store_true', help='output srppac strip data without calibration')
    args = parser.parse_args()

    # srppac_processor.py filename
    fname = args.input_file

    # stem = filename - extension
    stem = Path(fname).stem

    # Create a spark session with GPU
    # spark.sql.shuffle.partitions = number of cores is optimal
    spark = SparkSession.builder.master("local[*]") \
            .config("spark.driver.memory","20g") \
            .config("spark.executor.memory","20g") \
            .config("spark.sql.shuffle.partitions","32") \
            .config("spark.rapids.sql.concurrentGpuTasks","2") \
            .config("spark.rapids.memory.pinnedPool.size","2g") \
            .config("spark.sql.files.maxPartitionBytes","512m") \
            .config("spark.kryo.registrator","com.nvidia.spark.rapids.GpuKryoRegistrator") \
            .config("spark.plugins","com.nvidia.spark.SQLPlugin") \
            .config("spark.rapids.memory.gpu.allocFraction","0.8") \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Register decoder UDF
    spark._jvm.decoders.HRTDCDecoder.registerUDF(spark._jsparkSession)
    spark._jvm.decoders.HRTDCUnpairedDecoder.registerUDF(spark._jsparkSession)

    # Open parquet File
    df = spark.read.parquet(fname).filter("tfId>0")

    df_srppac = decode_srppac_amaneq_rpa(spark, df)
    if not args.output_strip_data:
        df_srppac = calib_srppac_dqdx(spark, df_srppac)

    # Write to parquet
    if args.output_file:
        ofname = args.output_file
    else:
        ofname = stem + ("_srppac.parquet")

    df_srppac = df_srppac.withColumn("runname",F.lit(stem))
    df_srppac.printSchema()
    df_srppac.write.mode("overwrite").parquet(ofname)