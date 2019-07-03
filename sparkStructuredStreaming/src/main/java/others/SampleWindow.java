package others;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import utils.Sparkconfig;

public class SampleWindow {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName(Sparkconfig.appName)
				.config("spark.sql.warehouse.dir", Sparkconfig.warehouse)
				.config("hive.exec.dynamic.partition.mode", Sparkconfig.partitionmode)
				.config("hive.metastore.uris", Sparkconfig.metastore_uris)
				.config("hive.enforce.bucketing", Sparkconfig.enforce_bucketing)
				.config("hive.enforce.sorting", Sparkconfig.enforce_sorting).enableHiveSupport().getOrCreate();
		
		StructType deviceSchema = new StructType().add("timestamp", "timestamp").add("animal", "string").add("numbers", "int");

		Dataset<Row> kpi_df = spark.readStream().option("sep", ",").schema(deviceSchema).option("header", "true")
				.csv("file:///home/bala/Documents/cango/files/stream/");
		
		Dataset<Row> windowedCounts = kpi_df
			    .withWatermark("timestamp", "10 seconds")
			    .groupBy(
			        functions.window(kpi_df.col("timestamp"), "10 seconds", "10 seconds"),
			        kpi_df.col("animal"))
			    .agg(functions.avg(kpi_df.col("numbers")), functions.max(kpi_df.col("numbers")),
			    		functions.min(kpi_df.col("numbers")),functions.sum(kpi_df.col("numbers")),
			    		functions.count(kpi_df.col("numbers")));
		
		StreamingQuery queryout = windowedCounts.writeStream().option("truncate","false").outputMode("update").format("console").start();

		try {
			queryout.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}
	}
}
