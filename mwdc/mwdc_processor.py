from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
import argparse
from pathlib import Path

CH2NS = 0.0009765625 # AMANEQ HRTDC time unit to ns

planes = ['dc31_x1','dc31_x2','dc31_y1','dc31_y2','dc31_x3','dc31_x4','dc31_y3','dc31_y4','dc32_x1','dc32_x2','dc32_y1','dc32_y2']
dc31_charge_range = [50,150]
dc31_timing_range = [-35,15]
dc32_charge_range = [50,150]
dc32_timing_range = [-35,15]
cell_size = 3.0
half_cell_size = cell_size / 2.0
planes_paires = [('dc31_x1', 'dc31_x2'), ('dc31_x3', 'dc31_x4'), ('dc31_y1', 'dc31_y2'), ('dc31_y3', 'dc31_y4'), ('dc32_x1', 'dc32_x2'), ('dc32_y1', 'dc32_y2')]
plane_shifts = [0.0, 0.0, 0.08, 0.15, -0.08, 0.0]
center_id1 = 7.25
center_id2 = 7.75
dc31_x3_shift = 0.0
dc31_y3_shift = 0.3

def decode_mwdc_amaneq(spark: SparkSession, df: DataFrame) -> DataFrame:
    # Filter MWDC data and decode
    df_dc = df.filter("femType==5 and (femId==616 or femId==617 or femId==618)").select("femId","data").withColumn("decoded",F.expr("decode_hrtdc_segdata(data)"))
    df_dc = df_dc.select("femId","decoded.*").select("femId","hbf.*","data").select("femId","hbfNumber","data").filter("array_size(decoded.data)>0")
    df_dc = df_dc.withColumn("ex",F.explode("data")).select("femId","hbfNumber","ex.*")
    df_dc = df_dc.withColumn("rand",F.rand().cast("float")).withColumn("tcal", (F.col("time").cast("float") + F.col("rand"))*F.lit(CH2NS).cast("float")).drop("time").drop("rand")
    df_dc = df_dc.withColumn("rand",F.rand().cast("float")).withColumn("charge", (F.col("tot").cast("float") + F.col("rand"))*F.lit(CH2NS).cast("float")).drop("tot").drop("rand")

    # +-------------+----------+----+-------+--------+-------+-------+-----+-----------+----------+--------+--------+--------------------+
    # |time_frame_id|num_source|type|version|   magic|   tfId|femType|femId|numMessages|   timeSec|timeUSec|subMagic|                data|
    # +-------------+----------+----+-------+--------+-------+-------+-----+-----------+----------+--------+--------+--------------------+
    # |            6|   1533192|   1|     83|SUBTIME|1533192|      7|  614|          4|1740605832|  838396|HRTBEAT|[08 65 17 00 00 0...|
    # |            6|   1533192|   1|     83|SUBTIME|1533192|      5|  616|          4|1740605832|  838851|HRTBEAT|[08 65 17 00 00 0...|
    # |            6|   1533192|   1|     83|SUBTIME|1533192|      5|  617|          4|1740605832|  838911|HRTBEAT|[08 65 17 00 00 0...|
    # |            6|   1533192|   1|     83|SUBTIME|1533192|      5|  618|          4|1740605832|  839650|HRTBEAT|[08 65 17 00 00 0...|
    # |            6|   1533192|   1|     83|SUBTIME|1533192|      5|  615|          4|1740605832|  840437|HRTBEAT|[08 65 17 00 00 0...|
    # +-------------+----------+----+-------+--------+-------+-------+-----+-----------+----------+--------+--------+--------------------+

    # Filter SRPPAC anode data and decode
    df_sra = df.filter("femType==5 and femId==615").select("data").withColumn("decoded",F.expr("decode_hrtdc_segdata(data)"))
    df_sra = df_sra.select("decoded.*").select("hbf.*","data").select("hbfNumber","data").filter("array_size(decoded.data)>0")
    df_sra = df_sra.withColumn("ex",F.explode("data")).select("hbfNumber","ex.*").filter("ch==0")
    df_sra = df_sra.withColumn("rand",F.rand().cast("float")).withColumn("tcal", (F.col("time").cast("float") + F.col("rand"))*F.lit(CH2NS).cast("float")).drop("time").drop("rand")
    df_sra = df_sra.withColumn("rand",F.rand().cast("float")).withColumn("sra_charge", (F.col("tot").cast("float") + F.col("rand"))*F.lit(CH2NS).cast("float")).drop("tot").drop("rand")
    df_sra = df_sra.withColumnRenamed("tcal","sra_timing").select("hbfNumber","sra_timing")

    # Select only the fastest sra hit per event
    from pyspark.sql import Window
    w = Window.partitionBy("hbfNumber").orderBy("sra_timing")
    df_sra = (
        df_sra
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # +-----+---------+---+------+------------------+
    # |femId|hbfNumber|ch |tot   |tcal              |
    # +-----+---------+---+------+------------------+
    # |616  |1540120  |53 |92642 |156542.81088314747|
    # |616  |1540120  |26 |89517 |156543.93596147845|
    # |616  |1540120  |58 |91948 |156547.63435481227|
    # |616  |1540120  |21 |87975 |156551.9335196402 |
    # |616  |1540120  |56 |85504 |156602.03707368436|
    # +-----+---------+---+------+------------------+

    # Subtract anode timing
    df_dc = df_dc.join(df_sra, on=["hbfNumber"])
    df_dc = df_dc.withColumn("tcal_c", F.expr("tcal-sra_timing")).drop("tcal").drop("sra_timing")

    # Map dc31 and dc32
    df_map31 = spark.read.csv("map/dc31_map.csv",inferSchema=True,header=True).withColumn("id",F.col("id").cast("int")).withColumn("femId",F.col("femId").cast("int")).withColumn("ch",F.col("ch").cast("int"))
    df_dc31 = df_dc.join(df_map31,on=["ch","femId"]).drop("ch").drop("femId")


    df_map32 = spark.read.csv("map/dc32_map.csv",inferSchema=True,header=True).withColumn("id",F.col("id").cast("int")).withColumn("femId",F.col("femId").cast("int")).withColumn("ch",F.col("ch").cast("int"))
    df_dc32 = df_dc.join(df_map32,on=["ch","femId"]).drop("ch").drop("femId")

    # +---------+-----+-------------------+---+
    # |hbfNumber|  tot|             tcal_c| id|
    # +---------+-----+-------------------+---+
    # |  1540120|92642|-137.10777327680262| 22|
    # |  1540120|89517|  -135.982694945822| 43|
    # |  1540120|91948| -132.2843016120023| 59|
    # |  1540120|87975|-127.98513678406016|  6|
    # |  1540120|85504| -77.88158273990848| 58|
    # +---------+-----+-------------------+---+

    # Separate planes and collect hits per event
    planes31 = ['x1','x2','y1','y2','x3','x4','y3','y4']
    NWIRE = 16
    df_planes31 = []
    for idx, plane in enumerate(planes31):
        df_plane = df_dc31.filter(f"id>={NWIRE*idx} AND id<{NWIRE*(idx+1)}")
        tname = "dc31_" + plane + "_timing"
        cname = "dc31_" + plane + "_charge"
        iname = "dc31_" + plane + "_id"
        df_plane = df_plane.withColumn(iname, F.col("id")-F.lit(NWIRE*idx))
        df_plane = df_plane.withColumnRenamed("tcal_c",tname)
        df_plane = df_plane.withColumnRenamed("charge",cname)
        df_plane = df_plane.filter(F.expr(f"{cname} > {dc31_charge_range[0]} and {cname} < {dc31_charge_range[1]} and {tname} > {dc31_timing_range[0]} and {tname} < {dc31_timing_range[1]}"))
        df_plane = df_plane.orderBy(F.col(cname).desc()).groupBy("hbfNumber").agg(F.collect_list(iname).alias(iname), F.collect_list(tname).alias(tname), F.collect_list(cname).alias(cname))
        df_planes31.append(df_plane)

    # df_planes31[0].show(5)
    # +---------+-------------+--------------------+--------------------+
    # |hbfNumber|        x1_id|           x1_timing|           x1_charge|
    # +---------+-------------+--------------------+--------------------+
    # |  1540119| [7, 3, 4, 5]|[-55.492256248835...|[91.6933354864662...|
    # |  1540120|       [6, 2]|[-127.98513678406...|[85.9139828961486...|
    # |  1540122|[4, 10, 8, 9]|[-125.91878912529...|[148.315700711490...|
    # |  1540123|   [9, 10, 5]|[-82.824411246780...|[93.7864164103644...|
    # |  1540124|    [7, 2, 5]|[-52.725479658809...|[91.4524336249308...|
    # +---------+-------------+--------------------+--------------------+

    planes32 = ['x1','x2','y1','y2']
    NWIRE = 16
    df_planes32 = []
    for idx, plane in enumerate(planes32):
        df_plane = df_dc32.filter(f"id>={NWIRE*idx} AND id<{NWIRE*(idx+1)}")
        tname = "dc32_" + plane + "_timing"
        cname = "dc32_" + plane + "_charge"
        iname = "dc32_" + plane + "_id"
        df_plane = df_plane.withColumn(iname, F.col("id")-F.lit(NWIRE*idx))
        df_plane = df_plane.withColumnRenamed("tcal_c",tname)
        df_plane = df_plane.withColumnRenamed("charge",cname)
        df_plane = df_plane.filter(F.expr(f"{cname} > {dc32_charge_range[0]} and {cname} < {dc32_charge_range[1]} and {tname} > {dc32_timing_range[0]} and {tname} < {dc32_timing_range[1]}"))
        df_plane = df_plane.orderBy(F.col(cname).desc()).groupBy("hbfNumber").agg(F.collect_list(iname).alias(iname), F.collect_list(tname).alias(tname), F.collect_list(cname).alias(cname))
        df_planes32.append(df_plane)


    # Join all planes
    df_dc31_planes = df_dc31.select("hbfNumber").dropDuplicates()
    for plane in df_planes31:
        df_dc31_planes = df_dc31_planes.join(plane, on=["hbfNumber"])

    df_dc32_planes = df_dc32.select("hbfNumber").dropDuplicates()
    for plane in df_planes32:
        df_dc32_planes = df_dc32_planes.join(plane, on=["hbfNumber"])

    df_mwdc = df_dc31_planes.join(df_dc32_planes, on=["hbfNumber"])
    df_mwdc = df_mwdc.join(df_sra,on=["hbfNumber"])
    return df_mwdc

def calib_mwdc_data(spark: SparkSession, df: DataFrame) -> DataFrame:
    # Calibrate MWDC data to get drift length for each plane
    rdf = df.select("hbfNumber")
    for plane in planes:
        # Create columns for the wire with loargest charge
        dfp = df.select("hbfNumber", f"{plane}_id",f"{plane}_charge",f"{plane}_timing")
        dfp = dfp.withColumn("charge0",F.expr(f"element_at({plane}_charge, 1)")) \
                 .withColumn("timing0",F.expr(f"element_at({plane}_timing, 1)")) \
                 .withColumn("id0",F.expr(f"element_at({plane}_id, 1)"))
        
        # Monotone converter
        df_conv = spark.read.csv(f"prm/{plane}_drift_calib_data.csv",inferSchema=True,header=True)
        df_conv = df_conv.withColumn("histy_x", F.col("histy_x").cast("float")) \
                         .withColumn("tx", F.col("tx").cast("float"))
        w = Window.orderBy(F.col("histy_x"))
        df_conv = df_conv.withColumn("histy_x_prev", F.lag("histy_x").over(w)) \
                         .withColumn("tx_prev", F.lag("tx").over(w)) \
                         .withColumn("row_number", F.row_number().over(w)) \
                         .filter(F.expr("row_number>1")) \
                         .drop("row_number")

        dfp = dfp.join(df_conv, (dfp.timing0 >= df_conv.histy_x_prev) & (dfp.timing0 < df_conv.histy_x), how="left")
        dfp = dfp.withColumn("randf", F.rand().cast("float")).withColumn(f"{plane}_dl", F.expr(f"(tx_prev + (tx - tx_prev)*randf)*{half_cell_size}f")).drop("randf")

        dfp = dfp.select("hbfNumber",f"{plane}_dl","id0","charge0","timing0").withColumnRenamed("id0",f"{plane}_id0").withColumnRenamed("charge0",f"{plane}_charge0").withColumnRenamed("timing0",f"{plane}_timing0")
        rdf = rdf.join(dfp, on=["hbfNumber"], how="left")

    # Calculate positions with shift corrections
    for i, plane_pair in enumerate(planes_paires):
        plane1 = plane_pair[0]
        plane2 = plane_pair[1]
        shift1 = 0.0
        shift2 = plane_shifts[i]

        diff = shift2 - shift1
        if (diff) > 0:
            rdf = rdf.withColumn(
                plane1 + "_posi",
                F.expr(f"""
                CASE
                    WHEN {plane1}_id0 != {plane2}_id0 THEN
                        (CAST({plane1}_id0 AS FLOAT) - CAST({center_id1} AS FLOAT) + {shift1/cell_size}f) * {cell_size}f + {plane1}_dl
                    WHEN {plane1}_id0 = {plane2}_id0 AND {plane2}_dl > {cell_size}f/2.0f - ABS({diff}f) THEN
                        (CAST({plane1}_id0 AS FLOAT) - CAST({center_id1} AS FLOAT) + {shift1/cell_size}f) * {cell_size}f + {plane1}_dl
                    ELSE
                        (CAST({plane2}_id0 AS FLOAT) - CAST({center_id2} AS FLOAT) + {shift2/cell_size}f) * {cell_size}f - {plane2}_dl     
                END
                """)
            ).withColumn(
                plane2 + "_posi",
                F.expr(f"""
                CASE
                    WHEN {plane1}_id0 != {plane2}_id0 THEN
                        (CAST({plane2}_id0 AS FLOAT) - CAST({center_id2} AS FLOAT) + {shift2/cell_size}f) * {cell_size}f - {plane2}_dl
                    WHEN {plane1}_id0 = {plane2}_id0 AND {plane1}_dl > {cell_size}f/2.0f - ABS({diff}f) THEN
                        (CAST({plane2}_id0 AS FLOAT) - CAST({center_id2} AS FLOAT) + {shift2/cell_size}f) * {cell_size}f - {plane2}_dl
                    ELSE
                        (CAST({plane2}_id0 AS FLOAT) - CAST({center_id2} AS FLOAT) + {shift2/cell_size}f) * {cell_size}f + {plane2}_dl     
                END        
                """)
            )
        else:
            rdf = rdf.withColumn(
                plane1 + "_posi",
                F.expr(f"""
                CASE
                    WHEN {plane1}_id0 = {plane2}_id0 THEN
                        (CAST({plane1}_id0 AS FLOAT) - CAST({center_id1} AS FLOAT) + {shift1/cell_size}f) * {cell_size}f - {plane1}_dl
                    WHEN {plane1}_id0 != {plane2}_id0 AND {plane2}_dl > {cell_size}f/2.0f - ABS({diff}f) THEN
                        (CAST({plane1}_id0 AS FLOAT) - CAST({center_id1} AS FLOAT) + {shift1/cell_size}f) * {cell_size}f - {plane1}_dl
                    ELSE
                        (CAST({plane1}_id0 AS FLOAT) - CAST({center_id1} AS FLOAT) + {shift1/cell_size}f) * {cell_size}f + {plane1}_dl     
                END
                """)
            ).withColumn(
                plane2 + "_posi",
                F.expr(f"""
                CASE
                    WHEN {plane1}_id0 = {plane2}_id0 THEN
                        (CAST({plane2}_id0 AS FLOAT) - CAST({center_id2} AS FLOAT) + {shift2/cell_size}f) * {cell_size}f + {plane2}_dl
                    WHEN {plane1}_id0 != {plane2}_id0 AND {plane2}_dl > {cell_size}f/2.0f - ABS({diff}f) THEN
                        (CAST({plane2}_id0 AS FLOAT) - CAST({center_id2} AS FLOAT) + {shift2/cell_size}f) * {cell_size}f - {plane2}_dl
                    ELSE
                        (CAST({plane2}_id0 AS FLOAT) - CAST({center_id2} AS FLOAT) + {shift2/cell_size}f) * {cell_size}f + {plane2}_dl     
                END
                """)
            )

    # Apply additional shifts for dc31 x3, x4 and y3, y4
    rdf = rdf.withColumn("dc31_x3_posi", F.expr(f"dc31_x3_posi + {dc31_x3_shift}f")) \
             .withColumn("dc31_x4_posi", F.expr(f"dc31_x4_posi + {dc31_x3_shift}f")) \
             .withColumn("dc31_y3_posi", F.expr(f"dc31_y3_posi + {dc31_y3_shift}f")) \
             .withColumn("dc31_y4_posi", F.expr(f"dc31_y4_posi + {dc31_y3_shift}f"))

    # Calculate final positions (average)
    rdf = rdf.withColumn("dc31_x", F.expr("(dc31_x1_posi + dc31_x2_posi + dc31_x3_posi + dc31_x4_posi)/4.0f")) \
             .withColumn("dc31_y", F.expr("(dc31_y1_posi + dc31_y2_posi + dc31_y3_posi + dc31_y4_posi)/4.0f")) \
             .withColumn("dc32_x", F.expr("(dc32_x1_posi + dc32_x2_posi)/2.0f")) \
             .withColumn("dc32_y", F.expr("(dc32_y1_posi + dc32_y2_posi)/2.0f"))

    return rdf

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Decode raw parquet files and process MWDC data')

    parser.add_argument('input_file', help='input file')
    parser.add_argument('--output-file', help='output file (default: [input_file]_mwdc.parquet)')
    parser.add_argument('--output-wire-data', action='store_true', help='Output wire-by-wire hit data for calibration')
    args = parser.parse_args()

    # mwdc_processor.py filename
    fname = args.input_file

    # stem = filename - extension
    stem = Path(fname).stem

    # Create a spark session
    #spark  = SparkSession.builder.master("local[*]") \
    #        .config("spark.driver.memory","30g") \
    #        .config("spark.executor.memory","30g") \
    #        .getOrCreate()

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

    # Open parquet File
    df = spark.read.parquet(fname).filter("tfId>0")

    df_mwdc = decode_mwdc_amaneq(spark, df)
    if not args.output_wire_data:
        df_mwdc = calib_mwdc_data(spark, df_mwdc)

    # Write to parquet
    if args.output_file:
        ofname = args.output_file
    else:
        ofname = stem + ("_mwdc.parquet")
    df_mwdc = df_mwdc.withColumn("runname",F.lit(stem))
    df_mwdc.printSchema()
    df_mwdc.write.mode("overwrite").parquet(ofname)
