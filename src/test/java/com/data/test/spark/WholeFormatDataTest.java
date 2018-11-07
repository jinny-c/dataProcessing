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

import java.io.UnsupportedEncodingException;
import java.util.*;

import static com.data.base.util.ConfigRef.*;


public final class WholeFormatDataTest implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(WholeFormatDataTest.class);

    private static final long serialVersionUID = 1L;

    private static JavaSparkContext jsc;
    private static LockCount instance = LockCount.getInstance();

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

        filePath = "file:/D:/workspace/lakala/POSP/logs/log4";
        resPath = "D:\\workspace\\lakala\\POSP\\logs\\result2";

        System.setProperty("HADOOP_USER_NAME", "hadoop");
        System.setProperty("hadoop.home.dir", "D:\\java\\hadoop-2.7.6\\");
        //System.setProperty("file.encoding", "GBK");

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
        
        String path = "hdfs://10.7.111.31:9000/input/log/";
        //path = "file:/D:/workspace/lakala/POSP/logs/log1/UnionWXHttpConnector_cnl_40.trc";
        //path = "D:\\workspace\\lakala\\POSP\\logs\\log4\\";
//        JavaPairRDD<LongWritable,Text> pair = jsc.hadoopFile(path, TextInputFormat.class, LongWritable.class, Text.class,4);
//        
//		JavaRDD<String> lines = pair.map(new Function<Tuple2<LongWritable, Text>, String>() {
//			@Override
//			public String call(Tuple2<LongWritable, Text> v1) throws Exception {
//                //String toGBK = new String(v1._2().getBytes(), 0, v1._2().getLength(), "GBK");
//                String toGBK = new String(v1._2().getBytes(), "GBK");
//                //System.out.println(toGBK);
//                //int count = instance.getCount();
//                //System.out.println(count+"=====aa======"+v1._1);
//                //System.out.println(count+"=====bb======"+toGBK);
//                return toGBK;
//			}
//		});
//		
//		//lines.collect().forEach(s->System.out.println("s="+s));
//		//JavaRDD<String> lines = jsc.textFile(path);
//        
//        JavaRDD<String> result = lines.filter(new Function<String, Boolean>(){
//
//            @Override
//            public Boolean call(String v1) throws Exception {
//                int bl1 = v1.indexOf("<body>");
//                int bl2 = v1.indexOf("</body>");
//                //int bl4 = v1.indexOf("<sub_appid>");
//                return bl1 > 0 || bl2 > 0;
//            }
//        });

        //result.saveAsTextFile("file:/D:/workspace/lakala/POSP/logs/result4");
        //result.repartition(1).saveAsTextFile("file:/D:/workspace/lakala/POSP/logs/result4");
        //System.exit(1);
        
        
        JavaPairRDD<String, String> wholeFileLine = jsc.wholeTextFiles(path);
        //JavaPairRDD<String, String> wholeFileLine = jsc.wholeTextFiles(filePath);

//        JavaRDD<String> whole = wholeFileLine.map(new Function<Tuple2<String,String>, String>() {
//
//			@Override
//			public String call(Tuple2<String, String> v1) throws Exception {
//				// TODO Auto-generated method stub
//				
//				System.out.println("======"+v1._2.getBytes().length);
//				System.out.println("======"+byteToString("GBK","GBK",v1._2));
//				return v1._2;
//			}
//		});
//		//System.out.println("======"+System.getProperty("file.encoding"));
//		//System.out.println("======"+System.getProperty("sun.jnu.encoding"));
//		whole.collect().forEach(s->System.out.println(s));
//		//whole.repartition(1).saveAsTextFile("D:\\workspace\\lakala\\POSP\\logs\\result12");
//        System.exit(1);
        
        
        JavaPairRDD<String, String> wholeLines = wholeFileLine.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>(){
			@Override
			public Iterator<Tuple2<String, String>> call(
					Tuple2<String, String> wholeFileLine) throws Exception {
		        String fileName = wholeFileLine._1;
		        String fileLine = wholeFileLine._2;
		        
		        ArrayList<Tuple2<String, String>> al = new ArrayList<Tuple2<String, String>>();
		        Arrays.asList(FileFormatConfig.PATTERN_XML.split(fileLine)).forEach(s -> al.add(new Tuple2<String, String>(fileName, s)));
//		        Arrays.asList(FileFormatConfig.PATTERN_XML.split(fileLine)).forEach(s->{
//		        	String ss = "";
//		        	try {
//						ss = new String(s.getBytes("GBK"),"UTF-8");
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//		        	al.add(new Tuple2<String, String>(fileName, ss));
//		        });
		        return al.iterator();
		        
			}
        	
        });
        
        //wholeFileLine.collect().forEach(s->System.out.println(s));
        wholeLines.repartition(1).saveAsTextFile("D:\\workspace\\lakala\\POSP\\logs\\result12");
        System.exit(1);
        
        
//        JavaRDD<String> resRDD2 = wholeFileLine.map(new Function<Tuple2<String, String>, String>() {
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public String call(Tuple2<String, String> v1)
//                    throws Exception {
//                return v1._1 + v1._2;
//            }
//        });
        
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
        
        JavaPairRDD<String, Map<String, String>> resPairRDD = filterLines.mapToPair(new PairFunction<Tuple2<String, String>, String, Map<String, String>>(){

			@Override
			public Tuple2<String, Map<String, String>> call(
					Tuple2<String, String> filterLine) throws Exception {
				
		        String fileName = filterLine._1;
		        String fileLine = filterLine._2;
		        
		        Map<String, String> valMap = new HashMap<String, String>();
		        int start = 0;
		        int end = 0;
		        
		        start = fileLine.indexOf(FileFormatConfig.out_trade_no_start);
		        end = fileLine.indexOf(FileFormatConfig.out_trade_no_end);
		        String outTradeNo = fileLine.substring(start + FileFormatConfig.out_trade_no_start.length()+1, end);
		        valMap.put(FileFormatConfig.out_trade_no, outTradeNo);
		        
		        valMap.put("values", new String(fileLine.getBytes("UTF-8"),"GBK"));
		        
		        return new Tuple2<String, Map<String, String>>(outTradeNo, valMap);
			}
        	
        });

        
        resPairRDD.repartition(1).saveAsTextFile("D:\\workspace\\lakala\\POSP\\logs\\result12");
        System.exit(1);
        
        
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

    private static String byteToString(String charset1,String charset2,String str){
    	
    	try {
			return new String(str.getBytes(charset1),charset2);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
    }
}
