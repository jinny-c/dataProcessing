package com.data.spark.conf;

import java.util.regex.Pattern;

public final class FileFormatConfig {


	public static final Pattern PATTERN_XML = Pattern.compile("<xml>|</xml>");

	public static final Pattern PATTERN_XML_1 = Pattern.compile("</xml>");
	public static final Pattern PATTERN_XML_2 = Pattern.compile("</xml>]");

	public static final String XML_START = "<xml>";
	public static final String XML_END = "</xml>";

	public static final String SWCP_NAME = "SWCT";
	public static final String SWCTCIBA_NAME = "SWCTCIBA_";
	public static final String UnionWX_NAME = "UnionWXHttpConnector_";
	public static final String WXHttp_NAME = "WXHttpConnector_";
	
	
	public static final String out_trade_no = "out_trade_no";
	public static final String out_trade_no_start = "<out_trade_no><![CDATA";
	public static final String out_trade_no_end = "]]></out_trade_no>";

	public static final String appid = "appid";
	public static final String appid_start = "<appid><![CDATA";
	public static final String appid_end = "]]></appid>";
	
	public static final String openid = "openid";
	public static final String openid_start = "<openid><![CDATA";
	public static final String openid_end = "]]></openid>";
	
	public static final String sub_appid = "sub_appid";
	public static final String sub_appid_start = "<sub_appid><![CDATA";
	public static final String sub_appid_end = "]]></sub_appid>";
	
	public static final String sub_openid = "sub_openid";
	public static final String sub_openid_start = "<sub_openid><![CDATA";
	public static final String sub_openid_end = "]]></sub_openid>";
	
	public static final String FILE_TYPE = "fileNameType";
	public static final String XML_VAL = "xmlVal";

}