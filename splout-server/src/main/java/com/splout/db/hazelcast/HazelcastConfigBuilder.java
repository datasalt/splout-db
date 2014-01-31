package com.splout.db.hazelcast;

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

import java.util.Arrays;

import com.hazelcast.config.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.splout.db.common.SploutConfiguration;

/**
 * Hazelcast configuration can be built programmatically as a facade using the SploutSQL configuration or it can be
 * loaded from hazelcast.xml in classpath or hazelcast.config env property. To do the latter, use
 * HazelcastProperties.USE_DEFAULT_OR_XML_CONFIG
 */
public class HazelcastConfigBuilder {

	private final static Log log = LogFactory.getLog(HazelcastConfigBuilder.class);

	@SuppressWarnings("serial")
	public static class HazelcastConfigBuilderException extends Exception {

		public HazelcastConfigBuilderException(String message) {
			super(message);
		}
	}

	public static Config build(SploutConfiguration buConf) throws HazelcastConfigBuilderException {
		// We can provide a Hazelcast XML
		if(buConf.getProperty(HazelcastProperties.USE_DEFAULT_OR_XML_CONFIG) != null) {
			// http://www.hazelcast.com/docs/1.9.4/manual/multi_html/ch11.html
			// We have to warn that some things will not be available (e.g. persistence)
			log.warn("Using hazelcast.config env variable or hazelcast.xml in classpath (http://www.hazelcast.com/docs/1.9.4/manual/multi_html/ch11.html). - Please note that persistence for some Coordination structures is not available unless manually provided by your .xml configuration.");
			return null; // Hazelcast will load the .xml
		}

		// Or we can configure it through our facade
		Config hzConfig = new Config();
		
		String strIFaces = buConf.getString(HazelcastProperties.INTERFACES);
		if(strIFaces != null) {
			log.info("-- Using Hazelcast network interfaces: " + strIFaces);
            InterfacesConfig iFaces = new InterfacesConfig();
			for(String strIFace : strIFaces.split(",")) {
				iFaces.addInterface(strIFace.trim());
			}
			hzConfig.getNetworkConfig().setInterfaces(iFaces);
		}
		
		Integer prop = buConf.getInteger(HazelcastProperties.PORT, -1);
		if(prop != -1) {
			log.info("-- Using Hazelcast port: " + prop);
			hzConfig.getNetworkConfig().setPort(prop);
		}

		// Default Backup Count and merge policy
		MapConfig cfg = new MapConfig();
		cfg.setName("default");
		cfg.setBackupCount(buConf.getInt(HazelcastProperties.BACKUP_COUNT));
		cfg.setMergePolicy("hz.LATEST_UPDATE");
		hzConfig.addMapConfig(cfg);

		cfg = new MapConfig();

		String joinMethod = buConf.getString(HazelcastProperties.JOIN_METHOD).toUpperCase();
		if(joinMethod == null) {
			throw new HazelcastConfigBuilderException("No join method specified in configuration, must be one of: "
			    + Arrays.toString(HazelcastProperties.JOIN_METHODS.values()));
		}

		HazelcastProperties.JOIN_METHODS method;
		try {
			method = HazelcastProperties.JOIN_METHODS.valueOf(joinMethod);
		} catch(IllegalArgumentException e) {
			throw new HazelcastConfigBuilderException("Invalid join method: " + joinMethod + " must be one of: "
			    + Arrays.toString(HazelcastProperties.JOIN_METHODS.values()));
		}

		NetworkConfig network = hzConfig.getNetworkConfig();
		JoinConfig join = network.getJoin();

		// Disable all by default. Then we enable the correct one.
		join.getMulticastConfig().setEnabled(false);
		join.getTcpIpConfig().setEnabled(false);
		join.getAwsConfig().setEnabled(false);

		// http://www.hazelcast.com/docs/1.9.4/manual/multi_html/ch11s02.html
		/*
		 * AWS autodiscovery
		 */
		if(method.equals(HazelcastProperties.JOIN_METHODS.AWS)) {
			log.info("Configuring Splout for AWS auto-discovery Hazelcast join (http://www.hazelcast.com/docs/1.9.4/manual/multi_html/ch11s02.html).");
			join.getAwsConfig().setEnabled(true);

			String key = buConf.getString(HazelcastProperties.AWS_KEY);
			if(key == null) {
				throw new HazelcastConfigBuilderException("Missing AWS Key property (" + HazelcastProperties.AWS_KEY + ")");
			}

			String secretKey = buConf.getString(HazelcastProperties.AWS_SECRET);
			if(secretKey == null) {
				throw new HazelcastConfigBuilderException("Missing AWS Secret Key property (" + HazelcastProperties.AWS_SECRET
				    + ")");
			}

			join.getAwsConfig().setAccessKey(key);
			join.getAwsConfig().setSecretKey(secretKey);

			// Optionally add the security group

			String securityGroup = buConf.getString(HazelcastProperties.AWS_SECURITY_GROUP);
			if(securityGroup != null) {
				log.info("-- Using security group: " + securityGroup);
				join.getAwsConfig().setSecurityGroupName(securityGroup);
			}
			/*
			 * TCP
			 */
		} else if(method.equals(HazelcastProperties.JOIN_METHODS.TCP)) {
			log.info("Configuring SploutSQL for TCP/IP Hazelcast join.");
			join.getTcpIpConfig().setEnabled(true);
			String tcpCluster = buConf.getString(HazelcastProperties.TCP_CLUSTER);

			if(tcpCluster == null) {
				throw new HazelcastConfigBuilderException("Enabled TCP join method but missing TCP Cluster key property ("
				    + HazelcastProperties.TCP_CLUSTER + ")");
			}

			String[] cluster = tcpCluster.split(",");
			for(String host : cluster) {
				try {
					join.getTcpIpConfig().addMember(host);
				} catch(Throwable e) {
		    	log.error("Invalid host in TCP cluster", e);
					throw new HazelcastConfigBuilderException("Invalid host in TCP cluster: " + host);
				}
			}

			/*
			 * MULTICAST
			 */
		} else {
			log.info("Configuring SploutSQL for MULTICAST Hazelcast join.");
			join.getMulticastConfig().setEnabled(true);

			String group = buConf.getString(HazelcastProperties.MULTICAST_GROUP);
			if(group != null) {
				log.info("-- Using multicast group: " + group);
				join.getMulticastConfig().setMulticastGroup(group);
			}

			Integer port = buConf.getInteger(HazelcastProperties.MULTICAST_PORT, -1);
			if(port != -1) {
				log.info("-- Using multicast port: " + port);
				join.getMulticastConfig().setMulticastPort(port);
			}
		}

		/*
		 * Miscelaneous
		 */
		if (buConf.getBoolean(HazelcastProperties.DISABLE_WAIT_WHEN_JOINING, false)) {
			log.info("Disabling Hazelcast join wait time.");
			hzConfig.setProperty("hazelcast.wait.seconds.before.join", "0");
		}
		
		return hzConfig;
	}
}
