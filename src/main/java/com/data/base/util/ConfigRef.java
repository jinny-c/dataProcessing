package com.data.base.util;

public final class ConfigRef {

	/** 参数配置 */
	public final static String ES_CLIENT_PORT = PropertiesUtil.getProperty("es.client.port");
	public final static String ES_CLIENT_IP = PropertiesUtil.getProperty("es.client.ip");
	public final static String ES_CLIENT_CLUSTER_NAME = PropertiesUtil.getProperty("es.client.cluster.name");


	public final static String SPARK_ES_NODES = PropertiesUtil.getProperty("spark.es.nodes");
	public final static String SPARK_ES_PORT = PropertiesUtil.getProperty("spark.es.port");
	public final static String SPARK_ES_CLUSTER_NAME = PropertiesUtil.getProperty("spark.es.cluster.name");

	public final static String ES_8583_INDEX_NAME = PropertiesUtil.getProperty("es.8583.index.name");
	public final static String ES_8583_INDEX_TYPE = PropertiesUtil.getProperty("es.8583.index.type");


}