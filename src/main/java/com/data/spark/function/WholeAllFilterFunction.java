package com.data.spark.function;

import com.data.spark.conf.FileFormatConfig;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * 过滤所需行
 */
public class WholeAllFilterFunction implements
        Function<Tuple2<String, String>, Boolean> {

    private static final Logger log = LoggerFactory
            .getLogger(WholeAllFilterFunction.class);
    private static final long serialVersionUID = 1L;

    @Override
    public Boolean call(Tuple2<String, String> line) throws Exception {
        // 文件名
        //String fileName = line._1;
        // 一整行
        String fileLine = line._2;

//        if (FileFormatConfig.UnionWX_NAME.equals(fileName)) {
//            return filterLineUnionWX(fileLine);
//        }

        return filterLine(fileLine);

    }

    // 处理SWCT的文件
    private Boolean filterLine(String line) {
        int bl1 = line.indexOf(FileFormatConfig.out_trade_no_start);
        int bl2 = line.indexOf(FileFormatConfig.appid_start);
        int bl3 = line.indexOf(FileFormatConfig.openid_start);

        return bl1 > 0 && bl2 > 0 && bl3 > 0;
    }

    // 处理UnionWX的文件
//    private Boolean filterLineUnionWX(String line) {
//        int bl1 = line.indexOf(FileFormatConfig.out_trade_no_start);
//        int bl2 = line.indexOf(FileFormatConfig.appid_start);
//        int bl3 = line.indexOf(FileFormatConfig.openid_start);
//        int bl4 = line.indexOf(FileFormatConfig.sub_appid_start);
//        int bl5 = line.indexOf(FileFormatConfig.sub_openid_start);
//        
//        return bl1 > 0 && bl2 > 0 && bl3 > 0 && bl4 > 0 && bl5 > 0;
//    }


}
