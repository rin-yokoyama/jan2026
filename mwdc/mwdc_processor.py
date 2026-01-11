from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
from pathlib import Path

# mwdc_processor.py filename
fname = sys.argv[1]
# stem = filename - extension
stem = Path(fname).stem

# Create a spark session
#spark  = SparkSession.builder.master("local[*]").getOrCreate()

# Create a spark session with GPU
spark = SparkSession.builder.master("local[*]") \
        .config("spark.rapids.sql.concurrentGpuTasks","1") \
        .config("spark.rapids.memory.pinnedPool.size","2g") \
        .config("spark.sql.files.maxPartitionBytes","512m") \
        .config("spark.kryo.registrator","com.nvidia.spark.rapids.GpuKryoRegistrator") \
        .config("spark.plugins","com.nvidia.spark.SQLPlugin") \
        .getOrCreate()

# Register decoder UDF
spark._jvm.decoders.HRTDCDecoder.registerUDF(spark._jsparkSession)

# Open parquet File
df = spark.read.parquet(fname).filter("tfId>0")

CH2NS = 0.0009765625

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
df_sra = df_sra.withColumn("ex",F.explode("data")).select("hbfNumber","ex.*").filter("ch==3")
df_sra = df_sra.withColumn("rand",F.rand().cast("float")).withColumn("tcal", (F.col("time").cast("float") + F.col("rand"))*F.lit(CH2NS).cast("float")).drop("time").drop("rand")
df_sra = df_sra.withColumn("rand",F.rand().cast("float")).withColumn("sra_charge", (F.col("tot").cast("float") + F.col("rand"))*F.lit(CH2NS).cast("float")).drop("tot").drop("rand")
#df_sra_t = df_sra.groupBy("hbfNumber").agg(F.min("tcal").alias("sra_timing")) # select fastest anode hit
#df_sra = df_sra.join(df_sra_t,on=["hbfNumber"]).drop("tcal").drop("ch")
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
F
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

# Write to parquet
ofname = stem + ("_mwdc.parquet")
df_mwdc.withColumn("runname",F.lit(stem))
df_mwdc.printSchema()
df_mwdc.write.mode("overwrite").parquet(ofname)
