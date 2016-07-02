package com.hiido.eagle.hes.dimport.common;

public class Conts {
	
	public static final byte FILE_TYPE_INDEX = 0x0;
	public static final byte FILE_TYPE_TRANSLOG = 0x1;
	public static final String FILE_TYPE_INDEX_FOLDER = "index";
	public static final String FILE_TYPE_TRANSLOG_FOLDER = "translog";
	
	public static final String PRO_LOCAL_TMP_PATH = "local.tmp.path";
	public static final String PRO_CLEAN_LOCAL_TMP_PATH = "clean.local.tmp.path";
	
	public static final String PRO_INDEX_NAME = "index.name";
	public static final String PRO_INDEX_TYPE = "index.type";
	public static final String PRO_AUTO_GENERATE_ID = "auto.generate.id";
	
	public static final byte TRANSFER_SUC = 0x0;
	public static final byte TRANSFER_ERR = 0x1;

}
