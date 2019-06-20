package utils;

public class Sparkconfig {
	
	public static final boolean is_object_level = true;
	public static final String appName = "KPI_data_loading";
	public static final String warehouse = "hdfs://localhost:9000/user/hive/warehouse/";
	public static final String partitionmode = "nonstrict";
	public static final String metastore_uris = "thrift://localhost:9083";
	public static final String enforce_bucketing = "true";
	public static final String enforce_sorting = "true";
	public static final String input_path = "file:///home/bala/Documents/cango/files/KPI/";
	public static final String hive_database = "balasrinivasan";
	public static final String hive_table = "network_element_kpi_nrt";
	

}
