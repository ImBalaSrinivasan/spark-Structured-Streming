package sparkStructuredStreaming;

import static org.apache.spark.sql.functions.array_join;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_extract;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConverters;
import scala.collection.Seq;
import utils.Sparkconfig;

public class KPIInput_old {
	
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

		Seq<String> concat_cols = JavaConverters
				.collectionAsScalaIterableConverter(new ArrayList<String>(Arrays.asList("core_id", "vendor_id")))
				.asScala().toSeq();

		Dataset<Row> pre_kpi_DF = csvDF.withColumn("kpi_name_value",
				concat(csvDF.col("counter_name"), lit("^"), csvDF.col("counter_value")));

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
				array_join(pre_kpi_2_DF.col("kpi_name_value"), " # "));
		
//		JavaRDD<String> temp = pre_kpi_3_DF.toJavaRDD().map(row -> {
//			String output = row.get(0).toString();
//			System.out.println(output);
//			return output;
//		});
		
		
		StreamingQuery queryout1 = pre_kpi_3_DF.writeStream().outputMode("update").foreach(new ForeachWriter<Row>() {

			@Override
			public boolean open(long partitionId, long version) {
				return true;
			}

			@Override
			public void close(Throwable errorOrNull) {
			}

			@Override
			public void process(Row value) {

				List<ArrayList<String>> formula_list = new ArrayList<ArrayList<String>>();
				formula_list.add(new ArrayList<String>(Arrays.asList("12345", "sample1",
						"MIN(Counter_84151147)/MAX(Counter_84151150)", "Counter_84151147/Counter_84151150")));
				formula_list.add(new ArrayList<String>(Arrays.asList("1234567", "sample2",
						"(MAX(Counter_84151147)+MIN(Counter_84151150))/AVG(Counter_84150585)",
						"(Counter_84151147+Counter_84151150)/Counter_84150585")));

				System.out.println("Entered...............");

//				for (ArrayList<String> formulae : formula_list) {

//					List<String> kpi_row_with_value = Arrays.asList(
//							value.getAs("core_id").toString(), 
//							value.getAs("vendor_id").toString(),
//							value.getAs("ne_type_id").toString(), 
//							value.getAs("ne_id").toString(), 
//							value.getAs("object_name").toString(),
//							value.getAs("ne_date").toString(), 
//							value.getAs("ne_hour").toString(), 
//							value.getAs("ne_minute").toString());

					
					StructField[] structFields = new StructField[]{
				            new StructField("core_id", DataTypes.StringType, true, Metadata.empty()),
				            new StructField("vendor_id", DataTypes.StringType, true, Metadata.empty()),
				            new StructField("ne_type_id", DataTypes.StringType, true, Metadata.empty()),
				            new StructField("ne_id", DataTypes.StringType, true, Metadata.empty()),
				            new StructField("object_name", DataTypes.StringType, true, Metadata.empty()),
				            new StructField("ne_date", DataTypes.StringType, true, Metadata.empty()),
				            new StructField("ne_hour", DataTypes.StringType, true, Metadata.empty()),
				            new StructField("ne_minute", DataTypes.StringType, true, Metadata.empty())
				    };

				    StructType structType = new StructType(structFields);

				    List<Row> rows = new ArrayList<>();
				    
				    System.out.println(structFields.length);
				    
				    Object[] objects = new Object[structFields.length];
			        objects[0] = "1";
			        objects[1] = "1";
			        objects[2] = "1";
			        objects[3] = "TLAGMSC1-MSX";
			        objects[4] = "Connection Type:lftlm";
			        objects[5] = "2018-07-11";
			        objects[6] = "12";
			        objects[7] = "30";
			        Row row = RowFactory.create(objects);
			        rows.add(row);

					Dataset<Row> pre_kpi_3_DF = spark.createDataFrame(rows, structType);
					
					pre_kpi_3_DF.show();
					
					StreamingQuery queryout = pre_kpi_3_DF.writeStream().outputMode("append").format("console").start();
					
					
//					String kpi_id = formulae.get(0);
//					String kpi_name = formulae.get(1);
//					String kpi_formula = formulae.get(2);
//					String kpi_collcted_lit = value.getAs("kpi_name_value").toString();
					
//					System.out.println(kpi_id + ", " + kpi_name + ", " + kpi_formula);
//					System.out.println(kpi_collcted_lit);
						

//					Dataset<Row> kpi_DF = pre_kpi_3_DF.withColumn("kpi_name", lit(formulae.get(1)))
//							.withColumn("kpi_id", lit(formulae.get(0)))
//							.withColumn("value",
//									regexp_extract(pre_kpi_3_DF.col("kpi_name_value"),
//											"(#\\s)(Call Completion Rate\\^)(\\w+)(\\s#)", 3).cast("int"))
//							.drop("kpi_name_value");

//				}
			}
		}).start();

		try {
			queryout1.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}

		Dataset<Row> kpi_DF = pre_kpi_3_DF
				.withColumn("kpi_name", lit("Call Completion Rate")).withColumn("kpi_id",
						lit("Call Completion Rate"))
				.withColumn("value", regexp_extract(pre_kpi_3_DF.col("kpi_name_value"),
						"(#\\s)(Call Completion Rate\\^)(\\w+)(\\s#)", 3).cast("int"))
				.drop("kpi_name_value");
		return kpi_DF;
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
