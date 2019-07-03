package nrtcounter;

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
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import utils.Sparkconfig;

public class CounterNRT {

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

		Dataset<Row> formula_df = readformula(spark);

		Dataset<Row> counter_formula_df = counter_value_aggregated_df.crossJoin(formula_df);

		Dataset<Row> final_kpi_value_df = counter_formula_df
				.withColumn("value",
						callUDF("valueCalculator", counter_formula_df.col("kpi_name_value"),
								counter_formula_df.col("raw_formula")))
				.drop("kpi_name_value").drop("formula").drop("raw_formula").drop("kpi_cal_val").select("ne_id",
						"measurement_name", "ne_name", "sub_ne_type", "ne_hour", "ne_minute", "ne_datetime", "kpi_name",
						"value", "customer_id", "region_id", "object_id", "tag", "object_name", "service_id",
						"ipsla_id", "value_string", "qos_id", "core_id", "vendor_id", "ne_type_id", "ne_date",
						"kpi_id");

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

	public static Dataset<Row> readformula(SparkSession spark) {
		Dataset<Row> formula_df = spark.read().option("header", "true").csv(Sparkconfig.formula_path);
		return formula_df;
	}

	@SuppressWarnings({ "unchecked", "serial" })
	public static void main(String[] args) {

		SparkSession spark = CounterNRT.createsession();

		StructType deviceSchema = CounterNRT.createschema();

		Dataset<Row> kpi_df = spark.readStream().option("sep", ",").schema(deviceSchema).option("header", "true")
				.csv(Sparkconfig.input_path).drop("formula_counters");

		System.out.println("New File came in at : " + new Timestamp(System.currentTimeMillis()));

		Dataset<Row> final_kpi_table = calculatekpi(kpi_df, spark);

		StreamingQuery queryout = final_kpi_table.writeStream().outputMode("update").format("console").start();

		writeToHive(final_kpi_table);

		try {
			queryout.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}

	}

}
