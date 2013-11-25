package com.splout.db.engine;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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

	SQLITE, MYSQL, REDIS;

	public final static String supportedEngines() {
		return Arrays.toString(Engine.values());
	}

	public static Engine getDefault() {
		return SQLITE;
	}
}
