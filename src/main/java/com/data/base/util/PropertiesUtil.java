package com.data.base.util;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class PropertiesUtil {

    private static final Logger log = LoggerFactory.getLogger(PropertiesUtil.class);

    private static Properties properties;

    private PropertiesUtil() {
    }

    static {
        try {
            if(isWindowsOS()){
            	log.info("load config-kidd.properties");
            	//windows 作为服务器启动项目
                //properties = loadProperties("classpath:prop/hadoop.properties");
            	properties = loadProperties("/prop/hadoop.properties");
            } else {
            	log.info("load config-prod.properties");
            	//生产环境配置
            	properties = loadProperties("/prop/hadoop.properties");
            }
        } catch (IOException e) {
            log.error("加载配置文件失败！", e);
        }
    }

    private static boolean isWindowsOS(){
        String osName = System.getProperty("os.name");
        return osName.toLowerCase().contains("windows");
    }

    /**
     * 取出Property。
     */
    private static String getValue(String key) {
        String systemProperty = System.getProperty(key);
        if (systemProperty != null) {
            return systemProperty;
        }
        return properties.getProperty(key);
    }

    /**
     * 取出String类型的Property,如果为Null则返回空串
     */
    public static String getProperty(String key) {
        String value = getValue(key);
        if (value == null) {
            return "";
        }
        return value;
    }

    /**
     * 取出String类型的Property.如果都为Null則返回Default值.
     */
    public static String getProperty(String key, String defaultValue) {
        String value = getValue(key);
        return value != null ? value : defaultValue;
    }


    /**
     * 载入多个文件, 文件路径使用Spring Resource格式.
     * @throws IOException 
     */
    private static Properties loadProperties(String... resourcesPaths) throws IOException {
        Properties props = new Properties();
        for (String location : resourcesPaths) {
            log.info("Loading properties file from path: {}", location);
            		
            //InputStream is = ClassLoader.getSystemResourceAsStream(location);
            //jar get properties
            InputStream  is = PropertiesUtil.class.getResourceAsStream(location);


            props.load(is);
            IOUtils.closeQuietly(is);
        }
        return props;
    }
    public static void main(String[] args) {
		System.out.println(ConfigRef.ES_CLIENT_CLUSTER_NAME);
	}
}