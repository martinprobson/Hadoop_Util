package com.gmail.martinprobson.hadoop.util;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;


public class Util {

	/**
	 * Dump the Hadoop configuration to a String.
	 * @param conf
	 * @return String containing all configuration keys/values.
	 */
	public static String dumpConfiguration(Configuration conf) {
		
		StringBuffer sb = new StringBuffer();
		for(Map.Entry<String, String> entry : conf) 
			sb.append(entry + "\n");
		return sb.toString();
	}

}
