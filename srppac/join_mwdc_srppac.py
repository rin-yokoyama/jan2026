from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='join mwdc parquet and srppac parquet')

    parser.add_argument('input_prefix', help='prefix of the input files (Assumed to be <prefix>_srppac.parquet and <prefix>_mwdc.parquet)')
    parser.add_argument('--output-file', help='output file (default: <input_prefix>_joined.parquet)')
    args = parser.parse_args()

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

    # Open parquet File
    srppac_fname = args.input_prefix + "_srppac.parquet"
    mwdc_fname = args.input_prefix + "_mwdc.parquet"
    df_srppac = spark.read.parquet(srppac_fname)
    df_srppac = df_srppac.select(["hbfNumber"]+[c for c in df_srppac.columns if c.endswith("_pos_x") or c.endswith("_pos_y")])
    df_mwdc = spark.read.parquet(mwdc_fname)
    df_mwdc = df_mwdc.select(["hbfNumber"]+[c for c in df_mwdc.columns if c.endswith("_x") or c.endswith("_y")])
    df = df_srppac.join(
        df_mwdc,
        on=["hbfNumber"]
    )


    # Write to parquet
    if args.output_file:
        ofname = args.output_file
    else:
        ofname = args.input_prefix + ("_joined.parquet")
    df.printSchema()
    df.write.mode("overwrite").parquet(ofname)
