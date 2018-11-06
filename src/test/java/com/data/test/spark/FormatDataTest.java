package com.data.test.spark;


import com.data.spark.conf.FileFormatConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

import static com.data.base.util.ConfigRef.*;


public final class FormatDataTest implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(FormatDataTest.class);

    private static final long serialVersionUID = 1L;

    private static JavaSparkContext jsc;

    public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();
        if (args.length < 3) {
            System.err.println("Usage: JavaWordCount <file>");
            //System.exit(1);
            args = new String[]{"path", "target", "indexType2"};
        }
        String filePath = args[0];
        String resPath = args[1];
        String indexType = args[2];
        String index_type = ES_8583_INDEX_NAME.concat("/").concat(indexType);

        filePath = "file:/D:/workspace/lakala/POSP/logs/log1";
        resPath = "D:\\workspace\\lakala\\POSP\\logs\\result2";

        System.setProperty("HADOOP_USER_NAME", "hadoop");
        System.setProperty("hadoop.home.dir", "D:\\java\\hadoop-2.7.6\\");

        SparkSession spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("FormatData8583Whole log")
                .config("es.index.auto.create", "true")
                .config("es.cluster.name", SPARK_ES_CLUSTER_NAME)
                .config("es.nodes", SPARK_ES_NODES)
                .config("es.port", SPARK_ES_PORT)
                //.config("es.cluster.name", "my-application")
                //.config("es.nodes", "http://lakalaDev")
                //.config("es.port", "9200")
                .getOrCreate();

        jsc = new JavaSparkContext(spark.sparkContext());
        JavaPairRDD<String, String> wholeFileLine = jsc.wholeTextFiles(filePath);

        JavaPairRDD<String, String> wholeLines = wholeFileLine.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {

            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, String> wholeFileLine) throws Exception {
                String name = wholeFileLine._1;
                String line = wholeFileLine._2;

                ArrayList<Tuple2<String, String>> al = new ArrayList<Tuple2<String, String>>();
                int start = 0;
                int end = 0;
                List<Tuple2<String, String>> xmlLine = new ArrayList<Tuple2<String, String>>();

                while (true) {
                    start = line.indexOf(FileFormatConfig.XML_START);
                    end = line.indexOf(FileFormatConfig.XML_END);

                    System.out.println("1===========" + start);
                    System.out.println("2===========" + end);

                    if (start <= 0 || end <= 0) {
                        //System.out.println("=========="+line);
                        break;
                    }

                    xmlLine.add(new Tuple2<String, String>(name, line.substring(start, end)));
                    line = line.substring(end);


                    System.out.println("3===========" + line.length());
                    System.out.println("4===========" + line.indexOf(FileFormatConfig.XML_START));
                    System.out.println("5===========" + line.indexOf(FileFormatConfig.XML_END));

                }

                return xmlLine.iterator();
            }
        });

        JavaRDD<String> resRDD2 = wholeFileLine.map(new Function<Tuple2<String, String>, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Tuple2<String, String> v1)
                    throws Exception {
                return v1._1 + v1._2;
            }
        });


        resRDD2.repartition(1).saveAsTextFile("D:\\workspace\\lakala\\POSP\\logs\\result21");
        System.exit(1);

        JavaPairRDD<String, String> filterLines = wholeLines.filter(new Function<Tuple2<String, String>, Boolean>() {

            @Override
            public Boolean call(Tuple2<String, String> line) throws Exception {

                String fileLine = line._2;
                int bl1 = fileLine.indexOf(FileFormatConfig.out_trade_no_start);
                int bl2 = fileLine.indexOf(FileFormatConfig.appid_start);
                int bl3 = fileLine.indexOf(FileFormatConfig.openid_start);

                return bl1 > 0 && bl2 > 0 && bl3 > 0;
            }
        });

        JavaPairRDD<String, Map<String, String>> resPairRDD = filterLines.mapToPair(new PairFunction<Tuple2<String, String>, String, Map<String, String>>() {

            @Override
            public Tuple2<String, Map<String, String>> call(Tuple2<String, String> line) throws Exception {
                String fileName = line._1;

                String fileLine = line._2;
                Map<String, String> valMap = new HashMap<String, String>();
                int start = 0;
                int end = 0;
                start = fileLine.indexOf("<out_trade_no><![CDATA");
                end = fileLine.indexOf("]]></out_trade_no>");
                valMap.put("out_trade_no", fileLine.substring(start + 23, end));

                valMap.put("sub_appid", "");
                valMap.put("sub_openid", "");

                return new Tuple2<String, Map<String, String>>(fileName, valMap);
            }
        });

        JavaRDD<Map<String, String>> resRDD = resPairRDD.map(new Function<Tuple2<String, Map<String, String>>, Map<String, String>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Map<String, String> call(Tuple2<String, Map<String, String>> v1)
                    throws Exception {
                Map<String, String> valMap = v1._2;
                valMap.put("fileType", v1._1);
                return valMap;
            }
        });

        JavaRDD<Map<String, String>> distinctRDD = resRDD.distinct();

        log.info("rdd count={},distinct={}", resRDD.count(), distinctRDD.count());

        distinctRDD.repartition(1).saveAsTextFile(resPath);

        //JavaEsSpark.saveToEs(distinctRDD, index_type);

        spark.stop();

        System.out.println("total time=" + (System.currentTimeMillis() - start));
    }

}
