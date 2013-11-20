package com.splout.db.engine;

import java.util.Arrays;

/**
 * The IDs of available engines in splout, used across modules.
 * <p>
 * For implementing a new engine one must provide an {@link EngineManager} implementation, which will be used by both
 * "server" and "hadoop" modules (for generation and serving), an OutputFormat and add its instantiation to the
 * appropriate factory in * each module. Each module has its own engine-related code in the same package
 * (com.splout.db.engine).
 */
public enum Engine {

	SQLITE, MYSQL;

	public final static String supportedEngines() {
		return Arrays.toString(Engine.values());
	}

	public static Engine getDefault() {
		return SQLITE;
	}
}
