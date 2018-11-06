package com.data.spark;


import com.data.spark.function.WholeAllFilterFunction;
import com.data.spark.function.WholeAllFlatMapToPair;
import com.data.spark.function.WholeAllMapToPair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.util.Map;

import static com.data.base.util.ConfigRef.*;

public final class WholeAllFormatData implements Serializable {

	private static final Logger log = LoggerFactory
			.getLogger(WholeAllFormatData.class);

	private static final long serialVersionUID = 1L;

	private static JavaSparkContext jsc;

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		if (args.length < 3) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
			//args = new String[] { "path", "target", "indexType2" };
		}
		String filePath = args[0];
		String resPath = args[1];
		String indexType = args[2];
		String index_type = ES_8583_INDEX_NAME.concat("/").concat(indexType);

		//filePath = "file:/D:/workspace/lakala/POSP/logs/log3";
		//resPath = "D:\\workspace\\lakala\\POSP\\logs\\result2";

		//System.setProperty("HADOOP_USER_NAME", "hadoop");
		//System.setProperty("hadoop.home.dir", "D:\\java\\hadoop-2.7.6\\");
		//System.setProperty("file.encoding","GBK");
		//System.setProperty("sun.jnu.encoding","GBK");
		//export SPARK_JAVA_OPTS=" -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 "
		//System.out.println("=========="+System.getProperty("file.encoding"));
		
		SparkSession spark = SparkSession.builder()
				//.master("local[4]")
				.appName("FormatData8583Whole log")
				.config("es.index.auto.create", "true")
				.config("es.cluster.name", SPARK_ES_CLUSTER_NAME)
				.config("es.nodes", SPARK_ES_NODES)
				.config("es.port", SPARK_ES_PORT)
				//.config("file.encoding", "GBK")
				//.config("sun.jnu.encoding", "GBK")
				//.config("spark.driver.extraJavaOptions", "file.encoding=GBK")
				//.config("spark.executor.extraJavaOptions", "file.encoding=GBK")
				// .config("es.cluster.name", "my-application")
				// .config("es.nodes", "http://lakalaDev")
				// .config("es.port", "9200")
				.getOrCreate();

		jsc = new JavaSparkContext(spark.sparkContext());
		
		JavaPairRDD<String, String> wholeFileLine = jsc
				.wholeTextFiles(filePath);
		
		//System.out.println("=========="+wholeFileLine.count());
		//wholeFileLine.collect();

		//wholeFileLine.repartition(1).saveAsTextFile("D:\\workspace\\lakala\\POSP\\logs\\result21");
		//System.exit(1);

		JavaPairRDD<String, String> wholeLines = wholeFileLine
				.flatMapToPair(new WholeAllFlatMapToPair());

		JavaPairRDD<String, String> filterLines = wholeLines
				.filter(new WholeAllFilterFunction());

		JavaPairRDD<String, Map<String, String>> resPairRDD = filterLines
				.mapToPair(new WholeAllMapToPair());

		JavaPairRDD<String, Map<String, String>> distinctResPairRDD = resPairRDD
				.reduceByKey(new Function2<Map<String, String>, Map<String, String>, Map<String, String>>() {
					public Map<String, String> call(Map<String, String> v1,
							Map<String, String> v2) throws Exception {
						if (v1.size() > v2.size()) {
							return v1;
						}
						return v2;
					}
				});

		JavaRDD<Map<String, String>> resRDD = distinctResPairRDD
				.map(new Function<Tuple2<String, Map<String, String>>, Map<String, String>>() {
					@Override
					public Map<String, String> call(
							Tuple2<String, Map<String, String>> v1)
							throws Exception {
						return v1._2;
					}
				});

		resRDD.repartition(1).saveAsTextFile(resPath);

		JavaEsSpark.saveToEs(resRDD, index_type);

		spark.stop();

		System.out.println("total time=" + (System.currentTimeMillis() - start));
	}

}
