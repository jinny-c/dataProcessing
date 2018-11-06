package com.data.spark.function;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 过滤所需行
 */
public class LineFunction implements Function<String, Boolean> {

    private static final Logger log = LoggerFactory.getLogger(LineFunction.class);
    private static final long serialVersionUID = 1L;

    @Override
    public Boolean call(String v1) throws Exception {
        //System.out.println("LineFunction start------");
        //log.info("LineFunction start------");
        return formateData(v1);
        //return test(v1);
    }

    private Boolean formateData(String v1) {
		// TODO Auto-generated method stub
        int bl1 = v1.indexOf("<out_trade_no>");
        int bl2 = v1.indexOf("<appid>");
        int bl3 = v1.indexOf("<openid>");
        //int bl4 = v1.indexOf("<sub_appid>");
        //int bl5 = v1.indexOf("<sub_openid>");
        return bl1 > 0 && bl2 > 0 && bl3 > 0 ;
    }
    
    private Boolean formateData1(String v1) {
    	
    	int bl1 = v1.indexOf("BUS_TYPE");
    	int bl2 = v1.indexOf("<OUT_TRADE_NO>");
    	int bl3 = v1.indexOf("<OPENID>");
    	int bl4 = v1.indexOf("<SUB_APPID>");
    	int bl5 = v1.indexOf("<SUB_OPENID>");
    	
    	return bl1 > 0 && bl2 > 0 && bl3 > 0 && bl4 > 0 && bl5 > 0;
    }
}
