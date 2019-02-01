/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.data.com.data.demo;

import java.text.DecimalFormat;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public final class FileDateFormat {
	
  public static void main(String[] args) throws Exception {
	  doSpark();
      testNumber();
  }
    private static void testNumber(){
        DecimalFormat df=new DecimalFormat(".0000");
        System.out.println(df.format((float)146237/26038));
        System.out.println((double) 146237/26038);
        System.out.println((float) 146237/26038);
    }
  private static void doSpark(){
	    String path = "file:/D:/workspace/lakala/POSP/logs/log2/";
	    path = "file:/C:\\Users\\jidd\\Desktop/abc.txt";
	    SparkSession spark = SparkSession
	      .builder().master("local[4]")
	      .appName("file date formate")
	      .getOrCreate();

	    JavaRDD<String> lines = spark.read().textFile(path).javaRDD();

	    formateFile(lines);

	    spark.stop();
  }
  private static void formateFile(JavaRDD<String> lines){
    JavaPairRDD<String, Integer> needs = lines.mapToPair(
            new PairFunction<String, String, Integer>() {
              @Override
              public Tuple2<String, Integer> call(String v1) {
                int c1 = v1.indexOf("wx notify end db1, time=");
                if (c1 > 0) {
                  return new Tuple2("db1",Integer.parseInt(v1.substring(c1 + 24)));
                }

                int c2 = v1.indexOf("wx notify end db2, time=");
                if (c2 > 0) {
                  return new Tuple2("db2",Integer.parseInt(v1.substring(c2 + 24)));
                }

                int c3 = v1.indexOf("wx notify end, time=");
                return new Tuple2("all",Integer.parseInt(v1.substring(c3 + 20)));
              }
            });

    JavaPairRDD<String, Integer> counts = needs.reduceByKey(
            new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
              }
            });
    
    Map<String, Long> needMap = needs.countByKey();

    counts.collect().forEach(t -> {
      System.out.println(t._1 + " counts " + t._2);
      Long c = needMap.get(t._1);
      System.out.println(t._1 + " average " + ((float)t._2 / c));
    });

    System.out.println("needMap = " + needMap);
  }
}
