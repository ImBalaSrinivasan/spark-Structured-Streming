package dataloading;

import java.sql.Timestamp;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import utils.Sparkconfig;

public class KPIDataLoading {

	public static SparkSession createsession() {

		SparkSession spark = SparkSession.builder().appName(Sparkconfig.appName)
				.config("spark.sql.warehouse.dir", Sparkconfig.warehouse)
				.config("hive.exec.dynamic.partition.mode", Sparkconfig.partitionmode)
				.config("hive.metastore.uris", Sparkconfig.metastore_uris)
				.config("hive.enforce.bucketing", Sparkconfig.enforce_bucketing)
				.config("hive.enforce.sorting", Sparkconfig.enforce_sorting)
				.enableHiveSupport().getOrCreate();

		return spark;
	}

	public static StructType createschema() {

		StructType deviceSchema = new StructType().add("ne_id", "string").add("ne_name", "string")
				.add("sub_ne_type", "string").add("ne_hour", "int").add("ne_minute", "int").add("ne_datetime", "string")
				.add("customer_id", "string").add("region_id", "int").add("core_id", "long").add("vendor_id", "long")
				.add("ne_type_id", "long").add("ne_date", "string").add("kpi_id", "long").add("kpi_name", "string")
				.add("value", "double").add("object_id", "string").add("ipsla_id", "int").add("service_id", "int")
				.add("object_name", "string").add("measurement_name", "string").add("tag", "int")
				.add("value_string", "string").add("formula_counters", "string").add("qos_id", "long");

		return deviceSchema;
	}


	@SuppressWarnings({ "unchecked", "serial" })
	public static void main(String[] args) {

		SparkSession spark = KPIDataLoading.createsession();

		StructType deviceSchema = KPIDataLoading.createschema();

		Dataset<Row> csvDF = spark.readStream().option("sep", ",").schema(deviceSchema).option("header", "true")
				.csv(Sparkconfig.input_path).drop("formula_counters");
		
		Timestamp start_time = new Timestamp(System.currentTimeMillis());
		System.out.println("New File came in at : " + new Timestamp(System.currentTimeMillis()));

		// to view DF
		StreamingQuery queryout = csvDF.writeStream().outputMode("append").format("console").start();

		StreamingQuery query = csvDF.writeStream().outputMode("append")
				.foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
					public void call(Dataset<Row> dataset, Long batchId) {
						dataset.write().mode("append").format("hive")
						.partitionBy("core_id", "vendor_id", "ne_type_id", "ne_date", "kpi_id")
						// .bucketBy(50, "kpi_id")
						.saveAsTable(Sparkconfig.hive_database + "." + Sparkconfig.hive_table);
						
						Timestamp end_time = new Timestamp(System.currentTimeMillis());
						System.out.println("New File came in at : " + new Timestamp(System.currentTimeMillis()));
						
						long time_diff = (start_time.getTime() - end_time.getTime())/ (60 * 1000) % 60;

						System.out.println("Written to Hive Table at : " + end_time);
						System.out.println("Completed batch no. - " + batchId.toString() + " in : " + time_diff + " Mins");
					}
				}).start();

		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}
	}
}
