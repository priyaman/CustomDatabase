package edu.buffalo.cse562.globals;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.sleepycat.je.Environment;

public class GlobalConstants {
	// FILE CONSTANTS
	public static String PREFIX_DATA_PATH;// = "./Sanity/data/";
	public static String PREFIX_DB_PATH;
	public static final String DOT = ".";
	public static final String PIPE = "|";
	public static final String SLASH = File.separator;
	public static final String DAT_SUFFIX = ".dat";
	public static final String JOIN_FILE = "JOIN_FILE";
	// DATA TYPE NAMES
	public static final String INTEGER_TYPE = "int";
	public static final String INTEGER_TYPE1 = "INT";
	public static final String LONG_TYPE = "LONG";
	public static final String DOUBLE_TYPE = "DECIMAL";
	public static final String STRING_TYPE = "STRING";
	public static final String VARCHAR_TYPE = "VARCHAR";
	public static final String CHAR_TYPE = "CHAR";
	public static final String FLOAT_TYPE = "FLOAT";
	public static final String DATE_TYPE = "DATE";

	public static Boolean HAS_SWAP= false;
	public static String SWAP_DIR;

	//phase names
	public static final String LOAD_PHASE = "LoadPhase";
	public static final String QUERY_PHASE = "QueryPhase";
	
	
	public static final String characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	// RA Operators
	public static enum RAOperator {
		SELECT, EXTENDED_PROJECT, JOIN, RELATION, GROUP_BY, AGGREGATE, UNION, ORDER_BY, PRINT
	};
	
	//FOR JOIN TYPES
	public static enum JoinTypes {
		HASH, MERGE, CROSS
	};

	// AGGREGATE FUNCTIONS
	public static final String SUM = "SUM";
	public static final String COUNT = "COUNT";
	public static final String AVG = "AVG";
	public static final String MIN = "MIN";
	public static final String MAX = "MAX";

	public static HashMap<String, Integer> datatypeSizeMap;
	static {
		datatypeSizeMap = new HashMap<String, Integer>();
		datatypeSizeMap.put(INTEGER_TYPE, (Integer.SIZE / Byte.SIZE));
		datatypeSizeMap.put(INTEGER_TYPE1, (Integer.SIZE / Byte.SIZE));
		datatypeSizeMap.put(LONG_TYPE, (Long.SIZE / Byte.SIZE));
		datatypeSizeMap.put(DOUBLE_TYPE, (Double.SIZE) / Byte.SIZE);
		datatypeSizeMap.put(CHAR_TYPE, (Character.SIZE) / Byte.SIZE);
		datatypeSizeMap.put(VARCHAR_TYPE, (Character.SIZE) / Byte.SIZE);
		datatypeSizeMap.put(FLOAT_TYPE, (Float.SIZE) / Byte.SIZE);
		datatypeSizeMap.put(DATE_TYPE, (Long.SIZE) / Byte.SIZE);

	}

	public static long BLOCK_SIZE = 8 * 9 / 8;// 40*1024*1024;
	public static int NUM_BLOCKS = 5;

	//project and select list
	public static List PROJECT_SELECT_LIST = new ArrayList<String>();
	
	//Index Environment
	public static Environment indexEnv = null;
}
