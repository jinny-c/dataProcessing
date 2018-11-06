package com.data.spark.function;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 格式化所需内容
 */
public class SaveEsFunction implements Function<String, Map<String, String>> {

    private static final Logger log = LoggerFactory.getLogger(SaveEsFunction.class);
    private static final long serialVersionUID = 1L;

    @Override
    public Map<String, String> call(String v1) throws Exception {
        //log.info("ValuesFunction start------");
        return formateData(v1);
    }

    private Map<String, String> formateData(String v1) {
        Map<String, String> valMap =  new HashMap<String, String>();

        int start = 0;
        int end = 0;
        start = v1.indexOf("<out_trade_no>");
        end = v1.indexOf("</out_trade_no>");
        valMap.put("out_trade_no", v1.substring(start + 14, end));

        start = v1.indexOf("<appid>");
        end = v1.indexOf("</appid>");
        valMap.put("appid", v1.substring(start + 7, end));
        
        start = v1.indexOf("<openid>");
        end = v1.indexOf("</openid>");
        valMap.put("openid", v1.substring(start + 8, end));
        
        valMap.put("sub_appid", "");
        valMap.put("sub_openid", "");

        return valMap;
    }
    
}
