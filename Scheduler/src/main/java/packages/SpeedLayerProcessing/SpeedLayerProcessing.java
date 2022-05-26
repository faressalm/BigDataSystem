package packages.SpeedLayerProcessing;

import org.apache.spark.sql.*;


import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import java.util.concurrent.TimeoutException;


import static packages.preprocessing.Wrapper.convert;
import static org.apache.spark.sql.functions.*;

public class SpeedLayerProcessing {

    public  void runTask(String inputPath) throws StreamingQueryException, InterruptedException, TimeoutException {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredNetworkWordCount").master("local[*]")
                .getOrCreate();
        StructType service = new StructType()
                .add("serviceName",DataTypes.IntegerType)
                .add("Timestamp",DataTypes.LongType)
                .add("CPU",DataTypes.DoubleType)
                .add("RAM_Total",DataTypes.DoubleType)
                .add("RAM_Free",DataTypes.DoubleType)
                .add("Disk_Total",DataTypes.DoubleType)
                .add("Disk_Free",DataTypes.DoubleType);

        Dataset<Row> lines = spark
                .readStream().format("json")
                .schema(service).json( convert(inputPath) );
        lines = lines.withColumn("ram_utl",col("RAM_Total").minus(col("RAM_Free")).divide(col("RAM_Total")))
                .withColumn("disk_utl",col("Disk_Total").minus(col("Disk_Free")).divide(col("Disk_Total")))
                .withColumn("Timestamp",col("Timestamp").divide(60).cast("long").multiply(60))
                .withColumnRenamed("serviceName","id");

        StreamingQuery query = lines
                .writeStream()
                .queryName("tempTable")
                .outputMode(OutputMode.Append())
                .format("memory")
                .trigger(Trigger.ProcessingTime(1_000))
                .start();
        query.awaitTermination(10000);
        Dataset<Row> parWrite = spark.sql("SELECT * FROM tempTable");
        Dataset<Row> ss = parWrite.groupBy(col("id"),col("Timestamp"))
                .agg(
                        sum("CPU").as("CPU"),
                        sum("ram_utl").as("RAM"),
                        sum("disk_utl").as("Disk"),
                        max("CPU").as("peakCPU"),
                        max("ram_utl").as("peakRAM"),
                        max("disk_utl").as("peakDisk"),
                        count("id").as("counter"));
        ss.write().mode(SaveMode.Append).parquet("honaaa.parquet");
        /*parWrite.write().format("console")
                .option("path", "tryparquet")
                .option("checkpointLocation", "chcekpoint");*/
        //parWrite.show(10);


    }
}
