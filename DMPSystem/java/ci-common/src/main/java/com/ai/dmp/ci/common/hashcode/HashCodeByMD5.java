package com.ai.dmp.ci.common.hashcode;

import org.apache.commons.codec.digest.DigestUtils;

public class HashCodeByMD5 {
	/*
	 * 通过MD5方式获取Hash值
	 * 
	 */
	public static String Hashcode(String ua) {
		return DigestUtils.md5Hex(ua).substring(8, 24);
	}
}
