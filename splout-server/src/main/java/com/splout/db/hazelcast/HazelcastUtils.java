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

import java.net.InetSocketAddress;
import java.util.ArrayList;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

public class HazelcastUtils {

	/**
	 * Return true if the given member is one of the oldest
	 * oldestCount members in the cluster. 
	 */
	public static boolean isOneOfOldestMembers(HazelcastInstance hz, Member member, int oldestCount) {
		ArrayList<Member> members = new ArrayList<Member>(hz.getCluster().getMembers());
		int idx = members.indexOf(member); 
		return idx != -1 && idx<oldestCount;
	}
	
	/**
	 * Return {@link InetSocketAddress#toString()} on the member
	 */
	public static String getHZAddress(Member member) {		
		return member.getInetSocketAddress().toString();		
	}
}
