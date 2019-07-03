package sparkStructuredStreaming;

import static org.apache.spark.sql.functions.array_join;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.sql.Timestamp;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import utils.Sparkconfig;

public class CounterAggregatedSecond {
	public static Dataset<Row> kpiDF = null;

	public static SparkSession createsession() {
		SparkSession spark = SparkSession.builder().appName(Sparkconfig.appName)
				.config("spark.sql.warehouse.dir", Sparkconfig.warehouse)
				.config("hive.exec.dynamic.partition.mode", Sparkconfig.partitionmode)
				.config("hive.metastore.uris", Sparkconfig.metastore_uris)
				.config("hive.enforce.bucketing", Sparkconfig.enforce_bucketing)
				.config("hive.enforce.sorting", Sparkconfig.enforce_sorting).enableHiveSupport().getOrCreate();
		return spark;
	}

	public static Dataset<Row> calculatekpi(Dataset<Row> kpi_df, SparkSession spark) {

		CalculatevalueNRT value_calculator = new CalculatevalueNRT();
		spark.udf().register("valueCalculator", value_calculator, DataTypes.StringType);

		Dataset<Row> counter_value_df = kpi_df.withColumn("kpi_name_value",
				concat(kpi_df.col("counter_id").cast("string"), lit("^"), kpi_df.col("counter_value")));

		Dataset<Row> counter_value_aggregated_df = counter_value_df
				.groupBy("core_id", "vendor_id", "ne_type_id", "ne_id", "object_name", "ne_date", "ne_hour",
						"ne_minute")
				.agg(collect_list("kpi_name_value")).withColumnRenamed("collect_list(kpi_name_value)", "kpi_name_value")
				.withColumn("customer_id", lit(null)).withColumn("region_id", lit(null))
				.withColumn("ipsla_id", lit(null)).withColumn("value_string", lit(null)).withColumn("qos_id", lit(null))
				.withColumn("service_id", lit(null)).withColumn("measurement_name", lit(null))
				.withColumn("ne_name", lit(null)).withColumn("sub_ne_type", lit(null))
				.withColumn("ne_datetime", lit(null)).withColumn("object_id", lit(null)).withColumn("tag", lit(null));

		counter_value_aggregated_df = counter_value_aggregated_df.withColumn("kpi_name_value",
				concat(lit("# "), array_join(counter_value_aggregated_df.col("kpi_name_value"), " # "), lit(" #")));

		Dataset<Row> formula_df = null;

		Dataset<Row> counter_formula_df = counter_value_aggregated_df.crossJoin(formula_df);

		Dataset<Row> final_kpi_value_df = counter_formula_df
				.withColumn("value",
						callUDF("valueCalculator", counter_formula_df.col("kpi_name_value"),
								counter_formula_df.col("raw_formula")))
				.drop("kpi_name_value").drop("formula").drop("raw_formula").drop("kpi_cal_val")
				.select("ne_id","measurement_name","ne_name","sub_ne_type","ne_hour","ne_minute",
						"ne_datetime","kpi_name","value","customer_id","region_id","object_id",
						"tag","object_name","service_id","ipsla_id","value_string","qos_id","core_id",
						"vendor_id","ne_type_id","ne_date","kpi_id");

		return final_kpi_value_df;
	}

	public static String writeToHive(Dataset<Row> df_to_write) {
		StreamingQuery query = df_to_write.writeStream().outputMode("update")
				.foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
					public void call(Dataset<Row> dataset, Long batchId) {
						dataset.write().mode("append").format("hive")
								.partitionBy("core_id", "vendor_id", "ne_type_id", "ne_date", "kpi_id")
								// .bucketBy(50, "kpi_id")
								.saveAsTable(Sparkconfig.hive_database + "." + Sparkconfig.hive_table);
					}
				}).start();
		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}

		return "Written to Hive..";

	}


	@SuppressWarnings({ "unchecked", "serial" })
	public static void main(String[] args) {

		SparkSession spark = CounterAggregated.createsession();

		System.out.println("New File came in at : " + new Timestamp(System.currentTimeMillis()));
		
		Dataset<Row> kpi_df = spark
				  .readStream()
				  .format("kafka")
				  .option("checkpointLocation", "file:///home/bala/Documents/cango/files/")
				  .option("kafka.bootstrap.servers", "localhost:9092")
				  .option("subscribe", "preaggcounter")
				  .option("startingOffsets", "earliest")
				  .load();
		
		kpi_df = kpi_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
		
		StructType deviceSchema1 = new StructType().add("window", "string").add("core_id", "int").add("vendor_id", "int")
				.add("ne_type_id", "int").add("ne_id", "string").add("object_name", "string")
				.add("counter_id", "long").add("avg_counter_value", "double").add("max_counter_value", "long")
				.add("min_counter_value", "long").add("sum_counter_value", "long")
				.add("avg_kpi_name_value", "string").add("max_kpi_name_value", "string")
				.add("min_kpi_name_value", "string").add("sum_kpi_name_value", "string");
		
		kpi_df = kpi_df.withColumn("value_json", functions.from_json(kpi_df.col("value"), deviceSchema1));
		
		kpi_df = kpi_df.withColumn("window", kpi_df.col("value_json").getField("window"))
				.withColumn("core_id", kpi_df.col("value_json").getField("core_id"))
				.withColumn("vendor_id", kpi_df.col("value_json").getField("vendor_id"))
				.withColumn("ne_type_id", kpi_df.col("value_json").getField("ne_type_id"))
				.withColumn("ne_id", kpi_df.col("value_json").getField("ne_id"))
				.withColumn("object_name", kpi_df.col("value_json").getField("object_name"))
				.withColumn("counter_id", kpi_df.col("value_json").getField("counter_id"))
				.withColumn("avg_counter_value", kpi_df.col("value_json").getField("avg_counter_value"))
				.withColumn("max_counter_value", kpi_df.col("value_json").getField("max_counter_value"))
				.withColumn("min_counter_value", kpi_df.col("value_json").getField("min_counter_value"))
				.withColumn("sum_counter_value", kpi_df.col("value_json").getField("sum_counter_value"))
				.withColumn("avg_kpi_name_value", kpi_df.col("value_json").getField("avg_kpi_name_value"))
				.withColumn("max_kpi_name_value", kpi_df.col("value_json").getField("max_kpi_name_value"))
				.withColumn("min_kpi_name_value", kpi_df.col("value_json").getField("min_kpi_name_value"))
				.withColumn("sum_kpi_name_value", kpi_df.col("value_json").getField("sum_kpi_name_value"))
				.drop("value_json").drop("value").drop("key");
				
		
		StreamingQuery queryout = kpi_df.writeStream().outputMode("update").option("truncate", "false").format("console").start();
		
		Dataset<Row> final_kpi_table = calculatekpi(kpi_df, spark);

		writeToHive(final_kpi_table);

		try {
			queryout.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}

		
	}
}
