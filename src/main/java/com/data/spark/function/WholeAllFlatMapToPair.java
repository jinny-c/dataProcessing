package com.data.spark.function;

import com.data.spark.conf.FileFormatConfig;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;


public class WholeAllFlatMapToPair implements
        PairFlatMapFunction<Tuple2<String, String>, String, String> {

    private static final Logger log = LoggerFactory
            .getLogger(WholeAllFlatMapToPair.class);
    private static final long serialVersionUID = 1L;

    @Override
    public Iterator<Tuple2<String, String>> call(
            Tuple2<String, String> wholeFileLine) throws Exception {
        // 文件名
        String fileName = wholeFileLine._1;
        // 整行
        String fileLine = wholeFileLine._2;
        //String fileLine = new String(wholeFileLine._2().getBytes("UTF-8"), 0, wholeFileLine._2().length(), "GBK");

        if (fileName.contains(FileFormatConfig.UnionWX_NAME)) {
            return splitLine(FileFormatConfig.PATTERN_XML, FileFormatConfig.UnionWX_NAME, fileLine);
        }
        if (fileName.contains(FileFormatConfig.WXHttp_NAME)) {
            return splitLine(FileFormatConfig.PATTERN_XML, FileFormatConfig.WXHttp_NAME, fileLine);
        }
        if (fileName.contains(FileFormatConfig.SWCTCIBA_NAME)) {
        	return splitLine(FileFormatConfig.PATTERN_XML, FileFormatConfig.SWCTCIBA_NAME, fileLine);
        }
        if (fileName.contains(FileFormatConfig.SWCP_NAME)) {
        	return splitLine(FileFormatConfig.PATTERN_XML, FileFormatConfig.SWCP_NAME, fileLine);
        }

        log.error("no such file name {}", fileName);
        return new ArrayList<Tuple2<String, String>>().iterator();
    }

    private Iterator<Tuple2<String, String>> splitLine(Pattern filePattern, String name, String line) {
        ArrayList<Tuple2<String, String>> al = new ArrayList<Tuple2<String, String>>();

        Arrays.asList(filePattern.split(line)).forEach(
                s -> al.add(new Tuple2<String, String>(name, s)));
        return al.iterator();
    }

}
