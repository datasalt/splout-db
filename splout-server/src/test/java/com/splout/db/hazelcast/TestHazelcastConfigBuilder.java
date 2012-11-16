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

import static org.junit.Assert.*;

import org.junit.Test;

import com.hazelcast.config.Config;
import com.splout.db.common.SploutConfiguration;
import com.splout.db.hazelcast.HazelcastConfigBuilder;
import com.splout.db.hazelcast.HazelcastProperties;
import com.splout.db.hazelcast.HazelcastConfigBuilder.HazelcastConfigBuilderException;

public class TestHazelcastConfigBuilder {

	@Test
	public void testDefaults() throws HazelcastConfigBuilderException {
		SploutConfiguration config = SploutConfiguration.getTestConfig();
		HazelcastConfigBuilder.build(config);
	}
	
	@Test
	public void testUseXML() throws HazelcastConfigBuilderException {
		SploutConfiguration config = SploutConfiguration.getTestConfig();
		config.setProperty(HazelcastProperties.USE_DEFAULT_OR_XML_CONFIG, true);
		HazelcastConfigBuilder.build(config);
	}
	
	@Test
	public void testAWS() throws HazelcastConfigBuilderException {
		SploutConfiguration config = SploutConfiguration.getTestConfig();
		config.setProperty(HazelcastProperties.JOIN_METHOD, "aws");
		config.setProperty(HazelcastProperties.AWS_KEY, "key");
		config.setProperty(HazelcastProperties.AWS_SECRET, "secret");
		config.setProperty(HazelcastProperties.AWS_SECURITY_GROUP, "sgroup");
		
		Config cfg = HazelcastConfigBuilder.build(config);
		
		assertEquals(true, cfg.getNetworkConfig().getJoin().getAwsConfig().isEnabled());
		assertEquals("key", cfg.getNetworkConfig().getJoin().getAwsConfig().getAccessKey());
		assertEquals("secret", cfg.getNetworkConfig().getJoin().getAwsConfig().getSecretKey());
		assertEquals("sgroup", cfg.getNetworkConfig().getJoin().getAwsConfig().getSecurityGroupName());
	}
	
	@Test
	public void testMulticast() throws HazelcastConfigBuilderException {
		SploutConfiguration config = SploutConfiguration.getTestConfig();
		config.setProperty(HazelcastProperties.JOIN_METHOD, "multicast");
		config.setProperty(HazelcastProperties.MULTICAST_PORT, "5432");
		config.setProperty(HazelcastProperties.MULTICAST_GROUP, "mgroup");
		
		Config cfg = HazelcastConfigBuilder.build(config);
		
		assertEquals(true, cfg.getNetworkConfig().getJoin().getMulticastConfig().isEnabled());
		assertEquals("mgroup", cfg.getNetworkConfig().getJoin().getMulticastConfig().getMulticastGroup());
		assertEquals(5432, cfg.getNetworkConfig().getJoin().getMulticastConfig().getMulticastPort());
	}
}
