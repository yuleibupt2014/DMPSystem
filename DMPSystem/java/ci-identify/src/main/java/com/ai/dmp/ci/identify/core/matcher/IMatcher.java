package com.ai.dmp.ci.identify.core.matcher;

import com.ai.dmp.ci.identify.core.Result;

public interface IMatcher {

	/**
	 * 匹配
	 * @param result
	 */
	public boolean match(Result result) throws Exception;
	
	/**
	 * 初始化
	 */
	public void initialize()throws Exception;
	
}
