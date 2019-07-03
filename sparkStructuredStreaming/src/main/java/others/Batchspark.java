package others;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import utils.Sparkconfig;

public class Batchspark {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName(Sparkconfig.appName)
				.config("spark.sql.warehouse.dir", Sparkconfig.warehouse)
				.config("hive.exec.dynamic.partition.mode", Sparkconfig.partitionmode)
				.config("hive.metastore.uris", Sparkconfig.metastore_uris)
				.config("hive.enforce.bucketing", Sparkconfig.enforce_bucketing)
				.config("hive.enforce.sorting", Sparkconfig.enforce_sorting).enableHiveSupport().getOrCreate();
		
		
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
		
		pre_kpi_3_DF = pre_kpi_3_DF.withColumn("json", functions.to_json(functions.array("core_id","vendor_id","ne_type_id")));
		
		pre_kpi_3_DF.selectExpr("to_json(struct(*)) AS value").show(false);
		
		pre_kpi_3_DF.select("json").show();
		

	}

}
