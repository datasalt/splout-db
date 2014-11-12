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

import com.hazelcast.core.*;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.splout.db.hazelcast.HazelcastUtils.getHZAddress;

/**
 * Creates a registry of members using a {@link IMap}. The user can create a {@link EntryListener} for
 * receiving events from the IMap wich inform of joining or leaving members. Some update events can be
 * generated, because of change in the information, or as cause of some re-registerings that are done for
 * safety. <br>
 * Main difficulty with the registry is the case of network partitions, or split-brain. In this case, all
 * registry replicas of one member could be lost. It is an improbable case, but possible. For this case,
 * we have a thread that periodically check that the node is registered so assuring eventual consistency.
 * </br> Another design principle of this registry is the idea of trying to minimize network
 * communication between nodes, as that could affect the system scalability.
 */
public class DistributedRegistry {

	private final static Log log = LogFactory.getLog(DistributedRegistry.class);

	private final String registryName;
	private Object nodeInfo;
	private final HazelcastInstance hzInstance;

	private final int minutesToCheckRegister;
	private final int oldestMembersLeading;

	private final AtomicBoolean amIRegistered = new AtomicBoolean(false);
	private final AtomicBoolean disableChecking = new AtomicBoolean(false);

	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	private final AtomicReference<ScheduledFuture<?>> checker = new AtomicReference<ScheduledFuture<?>>();
	private final Random random = new Random();

	public class MyListener implements MembershipListener {

		@Override
		public void memberAdded(MembershipEvent membershipEvent) {
			// Nothing to do. I should be registered. Just in case...
			scheduleCheck();
		}

		@Override
		public void memberRemoved(MembershipEvent membershipEvent) {
			synchronized(DistributedRegistry.this) {
				/*
				 * When a node leaves, somebody in the cluster must remove its info from the distributed map. We
				 * restrict this removal to some of the oldest members, in order to reduce coordination traffic
				 * while keeping replication
				 */
				if(HazelcastUtils.isOneOfOldestMembers(hzInstance, hzInstance.getCluster().getLocalMember(),
				    oldestMembersLeading)) {
					String member = getHZAddress(membershipEvent.getMember());
					log.info("Member " + member + " leaves. Unregistering it from registry [" + registryName + "]");
					ConcurrentMap<String, DNodeInfo> members = hzInstance.getMap(registryName);
					members.remove(member);
				}
			}
			// Just in case...
			scheduleCheck();
		}

		/**
		 * In the case of a network partition where some data is lost (unprovable, as replication should
		 * mitigate that), we could have some members that believe they are registered but they are not. We
		 * set a periodical check to test that. We schedule a test each time a new member arrives or leaves
		 * to the cluster. But in order to avoid to much checking, only one check can be performed in a
		 * period of time. Also, to reduce the herd behavior, we schedule the check randomly in this period.
		 */
		private void scheduleCheck() {
			if(disableChecking.get()) {
				return;
			}

			ScheduledFuture<?> checkerFuture = checker.get();

			if(checkerFuture == null || checkerFuture.isDone()) {
				int seconds = random.nextInt(Math.max(1, minutesToCheckRegister * 60));

				checker.set(scheduler.schedule(new Runnable() {

					@Override
					public void run() {
						synchronized(DistributedRegistry.this) {
							if(amIRegistered.get()) {
								String member = localMember();
								log.info("Checking if registered [" + member + "] ...");
								ConcurrentMap<String, Object> members = hzInstance.getMap(registryName);
								if(members.get(member) == null) {
									log.warn("Detected wrongly unregistered ["
									    + member
									    + "]. Could be due a network partition problem or due to a software bug. Reregistering.");
									register();
								}
							}
						}
					}
				}, seconds, TimeUnit.SECONDS));
			}
		}

		@Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
	    // TODO Since HZ 3.2
    }
	}

	/**
	 * When a split-brain happens, and two clusters will merge, the member of the smallest cluster are
	 * restarted. When we detect that, we reregister the clusters in order to assure that their info is
	 * present in the registry distributed map. <br>
	 * This alone does not assure complete coherence in the case of a network partition merge, as members
	 * of the bigger cluster could have an incomplete registry of themselves as well.
	 */
	public class RestartListener implements LifecycleListener {

		@Override
		public void stateChanged(LifecycleEvent event) {
			if(event.getState() == LifecycleState.MERGED) {
				synchronized(DistributedRegistry.this) {
					if(amIRegistered.get()) {
						log.info("Hazelcast RESTARTED event received. Reregistering myself to ensure I'm properly registered");
						register();
					}
				}
			}
		}
	}

	public DistributedRegistry(String registryName, Object nodeInfo, HazelcastInstance hzInstance,
	    int minutesToCheckRegister, int oldestMembersLeading) {
		this.registryName = registryName;
		this.nodeInfo = nodeInfo;
		this.hzInstance = hzInstance;
		hzInstance.getCluster().addMembershipListener(new MyListener());
		hzInstance.getLifecycleService().addLifecycleListener(new RestartListener());

		this.minutesToCheckRegister = minutesToCheckRegister;
		this.oldestMembersLeading = oldestMembersLeading;
	}

	public synchronized void register() {
		String myself = localMember();
		log.info("Registering myself [" + myself + "] on registry [" + registryName + "]");
		ConcurrentMap<String, Object> members = hzInstance.getMap(registryName);
		members.put(myself, nodeInfo);
		amIRegistered.set(true);
	}

	public synchronized void unregister() {
		String myself = localMember();
		log.info("Unregistering myself [" + myself + " -> " + nodeInfo + "] on registry [" + registryName
		    + "]");
		ConcurrentMap<String, Object> members = hzInstance.getMap(registryName);
		members.remove(myself);
		amIRegistered.set(false);
	}

	public synchronized void changeInfo(Object nodeInfo) {
		// Changing memory information. Needed for future reregistration
		this.nodeInfo = nodeInfo; 		
		String myself = localMember();
		log.info("Changing my info [" + myself + "] on registry [" + registryName + "]");
		ConcurrentMap<String, Object> members = hzInstance.getMap(registryName);
		members.put(myself, nodeInfo);
		amIRegistered.set(true);
	}

	/**
	 * Enables or disable preventive registration checking.
	 */
	protected void disableChecking(boolean disable) {
		disableChecking.set(disable);
	}

	private String localMember() {
		return getHZAddress(hzInstance.getCluster().getLocalMember());
	}

	public void dumpRegistry() {
		ConcurrentMap<String, Object> members = hzInstance.getMap(registryName);
		System.out.println("Registry [" + registryName + "] {");
		for(Entry<String, Object> entry : members.entrySet()) {
			System.out.println("\t" + entry.getKey() + " -> " + entry.getValue());
		}
		System.out.println("}");
	}
}