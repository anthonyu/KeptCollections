/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.killa.kept;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * Keeps a {@link Synchronizable} synchronized with the {@link ZooKeeper}
 * cluster.
 */
class SynchronizingWatcher implements Watcher {
    private static final Logger LOG = Logger
	    .getLogger(SynchronizingWatcher.class);

    private final Synchronizable synchronizable;

    /**
     * Instantiate a SynchronizeWatcher.
     * 
     * @param synchronizable
     *            A {@link Synchronizable} that will be kept synchronized
     */
    public SynchronizingWatcher(Synchronizable synchronizable) {
	this.synchronizable = synchronizable;
    }

    /**
     * Synchronize a {@link Synchronizable} with the {@link ZooKeeper} cluster
     * when a NodeChildrenChangedEvent is detected, or the client reconnects to
     * the cluster.
     * 
     * @param event
     *            A {@link WatchedEvent} that triggers synchronization
     * 
     */
    @Override
    public void process(WatchedEvent event) {
	// ignore no-op events and states in which we cannot read from the zk
	// cluster
	if (event.getType() == EventType.None
		|| event.getState() == KeeperState.Disconnected
		|| event.getState() == KeeperState.Expired) {
	    SynchronizingWatcher.LOG.debug("ignoring no-op event "
		    + event.getType() + " in state " + event.getState());

	    return;
	}

	try {
	    // synchronize the target
	    SynchronizingWatcher.LOG.debug("synchronizing");

	    this.synchronizable.synchronize();

	    return;
	} catch (KeeperException e) {
	    throw new RuntimeException("KeeperException caught", e);
	} catch (InterruptedException e) {
	    throw new RuntimeException("InterruptedException caught", e);
	}
    }
}
