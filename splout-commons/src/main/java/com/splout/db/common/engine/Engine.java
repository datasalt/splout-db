package com.splout.db.common.engine;

import java.util.Arrays;

public enum Engine {

	SQLITE, MYSQL;
	
	public final static String supportedEngines() {
		return Arrays.toString(Engine.values());
	}
	
	public static Engine getDefault() {
		return SQLITE;
	}
}
