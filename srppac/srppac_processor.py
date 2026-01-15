from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
import argparse
from pathlib import Path

CH2NS = 0.0009765625 # AMANEQ HRTDC time unit to ns
TIME_RANGE_NS = (-50, 100) # valid time range for SRPPAC strip hits after anode time subtraction and ns2ns conversion
HIT_SIZE_LIMIT = 7 # maximum number of hit strips per event to accept
HALF_STRIP_SIZE = (2.55 / 2.0, -2.58 / 2.0) # [x,y] mm
CENTER_ID = (16, 16) # [x,y] strip id
CALIB_RUNNAME = 'run1027'


def decode_srppac_amaneq(spark: SparkSession, df: DataFrame, preamp_type: str) -> DataFrame:
    # Filter SRPPAC strip data and decode
    df_src = df.filter("femType==7 and femId==615").select("data").withColumn("decoded",F.expr("decode_hrtdc_unpaired_segdata(data)"))
    df_src = df_src.select("decoded.*").select("hbf.*","data").select("hbfNumber","data").filter("array_size(decoded.data)>0")
    df_src = df_src.withColumn("ex",F.explode("data")).select("hbfNumber","ex.*").withColumnRenamed("tot","edge")
    df_src = df_src.withColumn("tcal", F.expr(f"(time + rand())*{CH2NS}")).drop("time")

    # Filter SRPPAC anode data and decode
    df_sra = df.filter("femType==5 and femId==614").select("data").withColumn("decoded",F.expr("decode_hrtdc_segdata(data)"))
    df_sra = df_sra.select("decoded.*").select("hbf.*","data").select("hbfNumber","data").filter("array_size(decoded.data)>0")
    df_sra = df_sra.withColumn("ex",F.explode("data")).select("hbfNumber","ex.*").filter("ch==4")
    df_sra = df_sra.withColumn("tcal", F.expr(f"(time + rand())*{CH2NS}")).drop("time")
    df_sra = df_sra.groupBy("hbfNumber").agg(F.min("tcal").alias("tcal_a")) # select fastest anode hit

    # Join strip and anode data
    df_sr = df_src.join(df_sra, on=["hbfNumber"])
    df_sr = df_sr.withColumn("tcal_c", F.expr("tcal-tcal_a")).drop("tcal").drop("tcal_a")
    df_sr = df_sr.filter(f"tcal_c > {TIME_RANGE_NS[0]} and tcal_c < {TIME_RANGE_NS[1]}")

    if preamp_type == "asagi":
        # Adjust tcal_c for asagi preamp type
        df_sr = df_sr.withColumn("edge", F.expr("CASE WHEN ch % 2 = 0 THEN 1 - edge ELSE edge END"))

    dfL = df_sr.filter("edge=0").select("hbfNumber","tcal_c","ch").withColumnRenamed("tcal_c","timingL")
    dfT = df_sr.filter("edge=1").select("hbfNumber","tcal_c","ch").withColumnRenamed("tcal_c","timingT")
    df_sr = dfL.join(dfT, on=["hbfNumber","ch"])
    df_sr = df_sr.withColumn("charge", F.expr("timingT - timingL"))

    # Map strip channel to rpa id for X
    df_xmap = spark.read.csv(f'map/srx_{preamp_type}_map.csv', inferSchema = True, header = True).withColumn("id", F.col("id").cast("int"))
    df_ymap = spark.read.csv(f'map/sry_{preamp_type}_map.csv', inferSchema = True, header = True).withColumn("id", F.col("id").cast("int"))

    # Apply strip-by-strip charge calibration
    df_prm_x = spark.read.csv(f'prm/srx_charge_calib_{preamp_type}.csv', inferSchema = True, header = True)
    df_prm_y = spark.read.csv(f'prm/sry_charge_calib_{preamp_type}.csv', inferSchema = True, header = True)

    dfs = [(df_xmap, df_prm_x), (df_ymap, df_prm_y)]
    df_xy_list = []
    for df_map, df_prm in dfs:
        df_xy = df_sr.join(df_map, on = ["ch"]).drop("ch")
        #df_xy = df_xy.join(df_prm, on=["id"], how="left")
        #df_xy = df_xy.withColumn("charge", F.expr("charge * p1 + p0")).drop("p1").drop("p0")

        df_xy = df_xy.orderBy(F.col("charge").desc()).groupBy("hbfNumber").agg(F.collect_list("id").alias("id"), F.collect_list("timingL").alias("timing"), F.collect_list("charge").alias("charge"))
        df_xy = df_xy.withColumn("size", F.expr("size(id)"))
        df_xy = df_xy.withColumn("timing0",F.expr(f"try_element_at(timing, 1)")) \
                     .withColumn("charge0",F.expr(f"try_element_at(charge, 1)")) \
                     .withColumn("charge1",F.expr(f"try_element_at(charge, 2)")) \
                     .withColumn("charge2",F.expr(f"try_element_at(charge, 3)")) \
                     .withColumn("id0",F.expr(f"try_element_at(id, 1)")) \
                     .withColumn("id1",F.expr(f"try_element_at(id, 2)")) \
                     .withColumn("id2",F.expr(f"try_element_at(id, 3)")) \
                     .withColumn("q0q1", F.expr("(charge0-charge1)/(charge0+charge1)"))
        
        df_xy_list.append(df_xy)

    # Add suffix _y to all columns except hbfNumber
    df_xy_list[0] = df_xy_list[0].select([F.col(col).alias(f"{col}_x") if col != "hbfNumber" else F.col(col) for col in df_xy_list[0].columns])
    df_xy_list[1] = df_xy_list[1].select([F.col(col).alias(f"{col}_y") if col != "hbfNumber" else F.col(col) for col in df_xy_list[1].columns])
    rdf = df_xy_list[0].join(df_xy_list[1], on=["hbfNumber"], how="outer")
    
    return rdf, df_sra

def calib_srppac_dqdx(spark: SparkSession, df: DataFrame) -> DataFrame:
    # Calibrate SRPPAC data
    # Number of hit strips should be between 2 and HIT_SIZE_LIMIT-1
    df = df.filter(f"size_x<{HIT_SIZE_LIMIT} and size_y<{HIT_SIZE_LIMIT}")
    df = df.filter("size_x>1 AND size_y>1")
    # Charge0 and Charge1 strips should be adjacent
    df = df.filter("ABS(id0_x - id1_x) = 1 AND ABS(id0_y - id1_y) = 1")

    dfs = [
        df.select("hbfNumber","id0_x","id1_x","charge0_x","charge1_x").withColumnRenamed("id0_x","id0").withColumnRenamed("id1_x","id1").withColumnRenamed("charge0_x","charge0").withColumnRenamed("charge1_x","charge1"),
        df.select("hbfNumber","id0_y","id1_y","charge0_y","charge1_y").withColumnRenamed("id0_y","id0").withColumnRenamed("id1_y","id1").withColumnRenamed("charge0_y","charge0").withColumnRenamed("charge1_y","charge1")
    ]
    planes = ["x","y"]

    rdf = df.select("hbfNumber")
    for i, dfxy in enumerate(dfs):
        # Define q0q1 for calibration parameter
        dfxy = dfxy.withColumn("q0q1", F.expr("(charge0 - charge1)/(charge0 + charge1)"))
        # Monotone converter
        df_conv = spark.read.csv(f"prm/srppac_q0q1_{planes[i]}_{CALIB_RUNNAME}.csv",inferSchema=True,header=True)
        df_conv = df_conv.withColumn("histy_x", F.col("histy_x").cast("float")) \
                         .withColumn("tx", F.col("tx").cast("float"))
        w = Window.orderBy(F.col("histy_x"))
        df_conv = df_conv.withColumn("histy_x_prev", F.lag("histy_x").over(w)) \
                         .withColumn("tx_prev", F.lag("tx").over(w)) \
                         .withColumn("row_number", F.row_number().over(w)) \
                         .filter(F.expr("row_number>1")) \
                         .drop("row_number")

        dfp = dfxy.join(df_conv, (dfxy.q0q1 >= df_conv.histy_x_prev) & (dfxy.q0q1 < df_conv.histy_x), how="left")
        dfp = dfp.withColumn("randf", F.rand().cast("float")).withColumn("ins", F.expr(f"(tx_prev + (tx - tx_prev)*randf)*ABS({HALF_STRIP_SIZE[i]}f)")).drop("randf")
        
        # Calculate position
        dfp = dfp.withColumn("ins", F.expr("CASE WHEN id1 = id0 + 1 THEN ins ELSE -ins END"))
        dfp = dfp.withColumn("pos", F.expr(f"({CENTER_ID[i]}f - id0) * ABS({HALF_STRIP_SIZE[i]}f)*2.0f + ins - {HALF_STRIP_SIZE[i]}f"))

        dfp = dfp.select("hbfNumber","pos","ins").withColumnRenamed("pos",f"sr_pos_{planes[i]}").withColumnRenamed("ins",f"sr_ins_{planes[i]}")
        rdf = rdf.join(dfp, on=["hbfNumber"], how="left")

    return rdf

def calib_srppac_dqdxlr(spark: SparkSession, df: DataFrame) -> DataFrame:
    # Calibrate SRPPAC data

    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='process srppac data from asagi')

    parser.add_argument('input_file', help='input file')
    parser.add_argument('--output-file', help='output file (default: [inputfile]_srppac.parquet)')
    parser.add_argument('--preamp-type', default='rpa', help='rpa/asagi')
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
            .config("spark.jars","/home/h487/opt/spark-oedo/scala_package/target/scala-2.13/spark-oedo-package_2.13-1.0.jar,/home/h487/opt/rapids/rapids-4-spark_2.13-25.10.0.jar") \
            .config("spark.rapids.sql.explain","NONE") \
            .config("spark.rapids.sql.concurrentGpuTasks","2") \
            .config("spark.rapids.memory.pinnedPool.size","2g") \
            .config("spark.sql.files.maxPartitionBytes","512m") \
            .config("spark.kryo.registrator","com.nvidia.spark.rapids.GpuKryoRegistrator") \
            .config("spark.plugins","com.nvidia.spark.SQLPlugin") \
            .config("spark.rapids.memory.gpu.allocFraction","0.8") \
            .config("spark.rapids.memory.gpu.minAllocFraction","0.01") \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Register decoder UDF
    spark._jvm.decoders.HRTDCDecoder.registerUDF(spark._jsparkSession)
    spark._jvm.decoders.HRTDCUnpairedDecoder.registerUDF(spark._jsparkSession)

    # Open parquet File
    df = spark.read.parquet(fname).filter("tfId>0")

    df_srppac, df_sra = decode_srppac_amaneq(spark, df, args.preamp_type)
    sra_count = df_sra.count()
    strip_count = df_srppac.filter('charge2_x > 0 AND charge2_y > 0 AND ABS(id0_x - id1_x) = 1 AND ABS(id0_y - id1_y) = 1 AND ABS(id0_x - id2_x) = 1 AND ABS(id0_y - id2_y) = 1').count()
    print(f"SRPPAC anode data count: {sra_count}")
    print(f"SRPPAC strip>2 data count: {strip_count}")
    print(f"SRPPAC efficiency: {strip_count/sra_count*100:.2f} %")
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
    print(f"Written calibrated SRPPAC data to {ofname}")
