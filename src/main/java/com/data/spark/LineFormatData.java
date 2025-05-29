package com.data.spark;


import com.data.spark.function.LineFunction;
import com.data.spark.function.SaveEsFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;

import java.util.Map;

import static com.data.base.util.ConfigRef.*;

public final class LineFormatData implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(LineFormatData.class);

    private static final long serialVersionUID = 1L;

    public static void main(String[] args) throws Exception {

    	long start = System.currentTimeMillis();
        if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <file>");
            //System.exit(1);
            args = new String[]{"filePath","resPath","indexType"};
        }

        String filePath = args[0];
        String resPath = args[1];
        String indexType = args[2];
        String index_type = ES_8583_INDEX_NAME.concat("/").concat(indexType);

        filePath = "file:/D:/Workers/workspace/logs/ori/";
       	resPath = "D:\\Workers\\workspace\\logs\\result";

        System.setProperty("HADOOP_USER_NAME", "hadoop");
        System.setProperty("hadoop.home.dir", "D:\\java\\hadoop-2.7.6\\");
        

        SparkSession spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("FormatData8583 log")
                .config("es.index.auto.create", "true")
                .config("es.cluster.name", SPARK_ES_CLUSTER_NAME)
                .config("es.nodes", SPARK_ES_NODES)
                .config("es.port", SPARK_ES_PORT)
                //.config("es.cluster.name", "my-application")
                //.config("es.nodes", "http://lakalaDev")
                //.config("es.port", "9200")
                .getOrCreate();


        JavaRDD<String> lines = spark.read().textFile(filePath).javaRDD();


        JavaRDD<String> resultLines = lines.filter(new LineFunction());

        JavaRDD<Map<String, String>> javaRDD = resultLines.map(new SaveEsFunction());

        JavaRDD<Map<String, String>> javaResRDD = javaRDD.distinct();

        JavaEsSpark.saveToEs(javaResRDD, index_type);

        javaResRDD.repartition(1).saveAsTextFile(resPath);

        spark.stop();

		System.out.println("total time=" + (System.currentTimeMillis() - start));
    }

}
