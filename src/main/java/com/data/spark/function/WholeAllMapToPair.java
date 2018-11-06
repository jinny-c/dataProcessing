package com.data.spark.function;

import com.data.spark.conf.FileFormatConfig;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * 过滤所需行
 */
public class WholeAllMapToPair implements
        PairFunction<Tuple2<String, String>, String, Map<String, String>> {

    private static final Logger log = LoggerFactory
            .getLogger(WholeAllMapToPair.class);
    private static final long serialVersionUID = 1L;


    @Override
	public Tuple2<String, Map<String, String>> call(Tuple2<String, String> line)
			throws Exception {
    	
		return substringLine(line._1, line._2);
	}

    // 处理SWCT的文件
	private Tuple2<String, Map<String, String>> substringLine(String fileName,
			String fileLine) {

        Map<String, String> valMap = new HashMap<String, String>();
        int start = 0;
        int end = 0;
        
        start = fileLine.indexOf(FileFormatConfig.out_trade_no_start);
        end = fileLine.indexOf(FileFormatConfig.out_trade_no_end);
        String outTradeNo = fileLine.substring(start + FileFormatConfig.out_trade_no_start.length()+1, end);
        valMap.put(FileFormatConfig.out_trade_no, outTradeNo);
        
        start = fileLine.indexOf(FileFormatConfig.appid_start);
        end = fileLine.indexOf(FileFormatConfig.appid_end);
        valMap.put(FileFormatConfig.appid, fileLine.substring(start + FileFormatConfig.appid_start.length()+1, end));

        start = fileLine.indexOf(FileFormatConfig.openid_start);
        end = fileLine.indexOf(FileFormatConfig.openid_end);
        valMap.put(FileFormatConfig.openid, fileLine.substring(start + FileFormatConfig.openid_start.length()+1, end));

        start = fileLine.indexOf(FileFormatConfig.sub_openid_start);
        if (start > 0) {
        	end = fileLine.indexOf(FileFormatConfig.sub_openid_end);
            valMap.put(FileFormatConfig.sub_openid, fileLine.substring(start + FileFormatConfig.sub_openid_start.length()+1, end));
        }else{
        	valMap.put(FileFormatConfig.sub_openid, "");
        }

        start = fileLine.indexOf(FileFormatConfig.sub_appid_start);
        if (start > 0) {
        	end = fileLine.indexOf(FileFormatConfig.sub_appid_end);
        	valMap.put(FileFormatConfig.sub_appid, fileLine.substring(start + FileFormatConfig.sub_appid_start.length()+1, end));
        }else{
        	valMap.put(FileFormatConfig.sub_appid, "");
        }
        
        //valMap.put(FileFormatConfig.XML_VAL, fileLine.replaceAll("\t|\r|\n", ""));
        valMap.put(FileFormatConfig.FILE_TYPE, fileName);

        return new Tuple2<String, Map<String, String>>(outTradeNo, valMap);
    }

}
