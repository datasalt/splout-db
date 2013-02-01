package com.splout.db.common;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.splout.db.dnode.DNodeProperties;
import com.splout.db.hazelcast.HazelcastProperties;
import com.splout.db.qnode.QNodeProperties;

/**
 * An Apache Commons Configuration object holding the configuration for DNode and QNode. The properties that are used
 * can be found, depending on its usage, in:
 * <ul>
 * <li>{@link com.splout.db.dnode.DNodeProperties}</li>
 * <li>{@link com.splout.db.dnode.FetcherProperties}</li>
 * <li>{@link com.splout.db.qnode.QNodeProperties}</li>
 * </ul>
 */
@SuppressWarnings("serial")
public class SploutConfiguration extends CompositeConfiguration implements Serializable {

	static Log log = LogFactory.getLog(SploutConfiguration.class);

	public final static String SPLOUT_PROPERTIES = "splout.properties";

	public static SploutConfiguration get() {
		return get(".");
	}
	
	/**
	 * Gets only the default values and adds some desirable properties for testing,
	 */
	public static SploutConfiguration getTestConfig() { 
		SploutConfiguration properties = new SploutConfiguration();
		PropertiesConfiguration config = load("", SPLOUT_PROPERTIES + ".default", true);
		properties.addConfiguration(config);

		// Disable wait for testing speedup.
		properties.setProperty(HazelcastProperties.DISABLE_WAIT_WHEN_JOINING, true);
		
		// Disable warming up - set it to only one second
		// that's enough since Hazelcast joining is by far slower
		properties.setProperty(QNodeProperties.WARMING_TIME, 1);
		
		// Disable HZ state storage
		properties.clearProperty(HazelcastProperties.HZ_PERSISTENCE_FOLDER);
		return properties;
	}

	/**
	 * Get the Splout configuration using double configuration: defaults + custom
	 */
	public static SploutConfiguration get(String rootDir) {
		SploutConfiguration properties = new SploutConfiguration();

		PropertiesConfiguration config = load(rootDir, SPLOUT_PROPERTIES, false);
		if(config != null) {
			properties.addConfiguration(config);
		}
		config = load(rootDir, SPLOUT_PROPERTIES + ".default", true);
		properties.addConfiguration(config);

		// The following lines replaces the default "localhost" by the local IP for convenience:
		String myIp = "localhost";

		try {
			Collection<InetAddress> iNetAddresses = GetIPAddresses.getAllLocalIPs();
			// but only if there is Internet connectivity!
			if(iNetAddresses != null) {
				Iterator<InetAddress> it = iNetAddresses.iterator();
				if(it.hasNext()) {
					InetAddress address = it.next();
					if(address.getHostAddress() != null) {
						myIp = address.getHostAddress();
					}
				}
			}
		} catch(IOException e) {
			throw new RuntimeException(e);
		}

		if(config.getString(QNodeProperties.HOST) != null
		    && config.getString(QNodeProperties.HOST).equals("localhost")) {
			config.setProperty(QNodeProperties.HOST, myIp);
		}

		if(config.getString(DNodeProperties.HOST) != null
		    && config.getString(DNodeProperties.HOST).equals("localhost")) {
			config.setProperty(DNodeProperties.HOST, myIp);
		}

		return properties;
	}

	// --------------- //

	private static URL loadAsResource(String what) {
		URL url = SploutConfiguration.class.getClassLoader().getResource(what);
		if(url == null) {
			url = ClassLoader.getSystemClassLoader().getResource(what);
			if(url != null) {
				log.info("Loading " + what + " from system classloader at: " + url);
			}
			return url;
		} else {
			log.info("Loading " + what + " from " + SploutConfiguration.class.getName() + " classloader at: "
			    + url);
		}
		return url;
	}

	private static PropertiesConfiguration load(String rootDir, String what, boolean strict) {
		File file = new File(new File(rootDir), what);
		if(!file.exists()) {
			URL url = loadAsResource(what);
			if(url == null) {
				if(strict) {
					throw new RuntimeException(what + " doesn't exist and it is not in the classpath.");
				} else {
					log.info(what + " doesn't exist.");
					return null;
				}
			} else {
				try {
					PropertiesConfiguration config = new PropertiesConfiguration(url);
					return config;
				} catch(ConfigurationException e) {
					throw new RuntimeException("Error loading default " + what);
				}
			}
		} else {
			log.info("Loading " + what + " from " + file);
			try {
				PropertiesConfiguration config = new PropertiesConfiguration(file);
				return config;
			} catch(ConfigurationException e) {
				throw new RuntimeException("Error loading default " + what);
			}
		}
	}

	private SploutConfiguration() {
		super();
	}
}