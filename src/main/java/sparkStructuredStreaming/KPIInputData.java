package sparkStructuredStreaming;

import static org.apache.spark.sql.functions.array_join;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConverters;
import scala.collection.Seq;
import utils.Sparkconfig;

public class KPIInputData {

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

	public static StructType createschema(boolean is_object_level) {
		StructType deviceSchema = null;
		if (is_object_level) {
			deviceSchema = new StructType().add("core_id", "int").add("vendor_id", "int").add("ne_type_id", "int")
					.add("ne_id", "string").add("ne_name", "string").add("object_name", "string")
					.add("ne_date", "string").add("ne_hour", "int").add("ne_minute", "int").add("ne_datetime", "string")
					.add("granularity", "int").add("measurement_id", "long").add("measurement_name", "string")
					.add("counter_id", "long").add("counter_name", "string").add("counter_value", "int")
					.add("tag", "string").add("sub_ne_type", "string").add("counter_string", "string");

		} else {

			deviceSchema = new StructType().add("ne_id", "string").add("ne_name", "string").add("sub_ne_type", "string")
					.add("ne_hour", "int").add("ne_minute", "int").add("ne_datetime", "string")
					.add("customer_id", "string").add("region_id", "int").add("core_id", "long")
					.add("vendor_id", "long").add("ne_type_id", "long").add("ne_date", "string").add("kpi_id", "long")
					.add("kpi_name", "string").add("value", "double").add("object_id", "string").add("ipsla_id", "int")
					.add("service_id", "int").add("object_name", "string").add("measurement_name", "string")
					.add("tag", "int").add("value_string", "string").add("formula_counters", "string")
					.add("qos_id", "long");
		}

		return deviceSchema;
	}

	public static Dataset<Row> calculatekpi(Dataset<Row> csvDF, SparkSession spark) {
		
		Calculatevalue cal_value = new Calculatevalue();
		
		spark.udf().register("calAvg", cal_value, DataTypes.StringType);

		Seq<String> concat_cols = JavaConverters
				.collectionAsScalaIterableConverter(new ArrayList<String>(Arrays.asList("core_id", "vendor_id")))
				.asScala().toSeq();

		Dataset<Row> pre_kpi_DF = csvDF.withColumn("kpi_name_value",
				concat(csvDF.col("counter_id").cast("string"), lit("^"), csvDF.col("counter_value")));

		Dataset<Row> pre_kpi_2_DF = pre_kpi_DF
				.groupBy("core_id", "vendor_id", "ne_type_id", "ne_id", "object_name", "ne_date", "ne_hour",
						"ne_minute")
				.agg(collect_list("kpi_name_value")).withColumnRenamed("collect_list(kpi_name_value)", "kpi_name_value")
				.withColumn("customer_id", lit(null)).withColumn("region_id", lit(null))
				.withColumn("ipsla_id", lit(null)).withColumn("value_string", lit(null)).withColumn("qos_id", lit(null))
				.withColumn("service_id", lit(null)).withColumn("measurement_name", lit(null))
				.withColumn("ne_name", lit(null)).withColumn("sub_ne_type", lit(null))
				.withColumn("ne_datetime", lit(null)).withColumn("object_id", lit(null)).withColumn("tag", lit(null));

		Dataset<Row> pre_kpi_3_DF = pre_kpi_2_DF.withColumn("kpi_name_value", 
				concat(lit("# "),array_join(pre_kpi_2_DF.col("kpi_name_value"), " # "), lit(" #"))
				);
		
		Dataset<Row> pre_kpi_4_DF = pre_kpi_3_DF;
		
		Dataset<Row> formula_df = readformula(spark);
		
		Dataset<Row> kpi_with_formula_df = pre_kpi_4_DF
				.crossJoin(formula_df);
		
		kpi_with_formula_df = kpi_with_formula_df
				.withColumn("kpi_cal_val", callUDF("calAvg", kpi_with_formula_df.col("kpi_name_value"),kpi_with_formula_df.col("raw_formula"))
						);
		
//		Dataset<Row> kpi_DF = pre_kpi_3_DF
//				.withColumn("kpi_name", lit("Call Completion Rate")).withColumn("kpi_id",
//						lit("Call Completion Rate"))
//				.withColumn("value", regexp_extract(pre_kpi_3_DF.col("kpi_name_value"),
//						"(#\\s)(Call Completion Rate\\^)(\\w+)(\\s#)", 3).cast("int"))
//				.drop("kpi_name_value");
		return kpi_with_formula_df;
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
		
		Dataset<Row> formula_df = spark.read()
				.option("header", "true")
				.csv("file:///home/bala/eclipse-workspace/sparkStructuredStreaming/src/main/resources/formula_sheet.csv");
		
		return formula_df;

	}

	@SuppressWarnings({ "unchecked", "serial" })
	public static void main(String[] args) {

		SparkSession spark = KPIInputData.createsession();

		StructType deviceSchema = KPIInputData.createschema(Sparkconfig.is_object_level);

		Dataset<Row> csvDF = spark.readStream().option("sep", ",").schema(deviceSchema).option("header", "true")
				.csv(Sparkconfig.input_path).drop("formula_counters");

		Timestamp start_time = new Timestamp(System.currentTimeMillis());
		System.out.println("New File came in at : " + new Timestamp(System.currentTimeMillis()));

		if (Sparkconfig.is_object_level) {
			Dataset<Row> kpi_without_cols_df = calculatekpi(csvDF, spark);

			StreamingQuery queryout = kpi_without_cols_df.writeStream().outputMode("update").format("console").start();

			// writeToHive(kpi_without_cols_df);

			try {
				queryout.awaitTermination();
			} catch (StreamingQueryException e) {
				e.printStackTrace();
			}

		} else {
			writeToHive(csvDF);

		}
	}
}
