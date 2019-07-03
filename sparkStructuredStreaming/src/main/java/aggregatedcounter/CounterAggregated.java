package aggregatedcounter;

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

import nrtcounter.CalculatevalueNRT;
import utils.Sparkconfig;

public class CounterAggregated {

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

	public static StructType createschema() {

		StructType deviceSchema = new StructType().add("core_id", "int").add("vendor_id", "int")
				.add("ne_type_id", "int").add("ne_id", "string").add("ne_name", "string").add("object_name", "string")
				.add("ne_date", "string").add("ne_hour", "int").add("ne_minute", "int").add("ne_datetime", "string")
				.add("granularity", "int").add("measurement_id", "long").add("measurement_name", "string")
				.add("counter_id", "long").add("counter_name", "string").add("counter_value", "int")
				.add("tag", "string").add("sub_ne_type", "string").add("counter_string", "string");

		return deviceSchema;
	}

	public static Dataset<Row> calculatekpi(Dataset<Row> kpi_df, SparkSession spark) {

		CalculatevalueNRT value_calculator = new CalculatevalueNRT();

		spark.udf().register("valueCalculator", value_calculator, DataTypes.StringType);

		kpi_df = kpi_df.withColumn("ne_datetime", kpi_df.col("ne_datetime").cast(DataTypes.TimestampType));

		Dataset<Row> kpi_window_df = kpi_df.withWatermark("ne_datetime", "15 minutes")
				.groupBy(functions.window(kpi_df.col("ne_datetime"), "15 minutes", "15 minutes"), kpi_df.col("core_id"),
						kpi_df.col("vendor_id"), kpi_df.col("ne_type_id"), kpi_df.col("ne_id"),
						kpi_df.col("object_name"), kpi_df.col("counter_id"))
				.agg(functions.avg(kpi_df.col("counter_value")), functions.max(kpi_df.col("counter_value")),
						functions.min(kpi_df.col("counter_value")), functions.sum(kpi_df.col("counter_value")));

		kpi_window_df = kpi_window_df.withColumnRenamed("avg(counter_value)", "avg_counter_value")
				.withColumnRenamed("max(counter_value)", "max_counter_value")
				.withColumnRenamed("min(counter_value)", "min_counter_value")
				.withColumnRenamed("sum(counter_value)", "sum_counter_value");

		Dataset<Row> counter_value_df = kpi_window_df
				.withColumn("avg_kpi_name_value",
						concat(kpi_window_df.col("counter_id").cast("string"), lit("^"),
								kpi_window_df.col("avg_counter_value")))
				.withColumn("max_kpi_name_value",
						concat(kpi_window_df.col("counter_id").cast("string"), lit("^"),
								kpi_window_df.col("max_counter_value")))
				.withColumn("min_kpi_name_value",
						concat(kpi_window_df.col("counter_id").cast("string"), lit("^"),
								kpi_window_df.col("min_counter_value")))
				.withColumn("sum_kpi_name_value", concat(kpi_window_df.col("counter_id").cast("string"), lit("^"),
						kpi_window_df.col("sum_counter_value")));

		Dataset<Row> counter_value_aggregated_df = counter_value_df.withColumn("customer_id", lit(null))
				.withColumn("region_id", lit(null)).withColumn("ipsla_id", lit(null))
				.withColumn("value_string", lit(null)).withColumn("qos_id", lit(null))
				.withColumn("service_id", lit(null)).withColumn("measurement_name", lit(null))
				.withColumn("ne_name", lit(null)).withColumn("sub_ne_type", lit(null)).withColumn("ne_date", lit(null))
				.withColumn("object_id", lit(null)).withColumn("tag", lit(null)).withColumn("ne_hour", lit(null))
				.withColumn("ne_minute", lit(null));

		return counter_value_aggregated_df;
	}

	public static Dataset<Row> readformula(SparkSession spark) {
		Dataset<Row> formula_df = spark.read().option("header", "true").csv(Sparkconfig.formula_path);
		return formula_df;
	}

	@SuppressWarnings({ "unchecked", "serial" })
	public static void main(String[] args) {

		SparkSession spark = CounterAggregated.createsession();

		StructType deviceSchema = CounterAggregated.createschema();

		Dataset<Row> kpi_df = spark.readStream().option("sep", ",").schema(deviceSchema).csv(Sparkconfig.input_path)
				.drop("formula_counters");

		System.out.println("New File came in at : " + new Timestamp(System.currentTimeMillis()));

		Dataset<Row> final_kpi_table = calculatekpi(kpi_df, spark);

		final_kpi_table.selectExpr("to_json(struct(*)) AS value").writeStream().format("kafka")
				.option("checkpointLocation", "file:///home/bala/Documents/cango/files/")
				.option("kafka.bootstrap.servers", "localhost:9092").option("topic", "preaggcounter")
				.outputMode("update").start();

		StreamingQuery queryout = final_kpi_table.writeStream().option("truncate", "false").outputMode("update")
				.format("console").start();

		try {
			queryout.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}

	}
}
