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

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * A Java {@link Lock} that is kept synchronized amongst a {@link ZooKeeper}
 * cluster.
 */
public class KeptLock implements Lock {
    private final ZooKeeper keeper;
    private final String znode;
    private final List<ACL> acl;

    /**
     * Construct a KeptLock.
     * 
     * @param keeper
     *            A {@link ZooKeeper} that is synchronized with
     * 
     * @param znode
     *            A {@link String} containing the znode that will serve as a
     *            lock
     * 
     * @param acl
     *            A {@link List} of {@link ACL} containing the access control
     *            lists for node creation
     */
    public KeptLock(ZooKeeper keeper, String znode, List<ACL> acl) {
	this.keeper = keeper;

	this.znode = znode;
	this.acl = acl;
    }

    private boolean lockIt(long t, TimeUnit tu) throws KeeperException,
	    InterruptedException {
	final CountDownLatch latch = new CountDownLatch(1);

	// convert the given time to milliseconds and add it to the current time
	long last = System.currentTimeMillis()
		+ TimeUnit.MILLISECONDS.convert(t, tu);
	do {
	    if (this.keeper.exists(this.znode, new Watcher() {
		@Override
		public void process(WatchedEvent event) {
		    if (event.getType() == EventType.NodeDeleted)
			latch.countDown();
		    else if (event.getType() == EventType.NodeCreated)
			; // ignore it
		    else
			throw new RuntimeException("unexpected event type"
				+ event.getType());
		}
	    }) != null) {
		if (!latch.await(t, tu))
		    try {
			this.keeper.create(this.znode, ManagementFactory.getRuntimeMXBean().getName().getBytes(), this.acl,
				CreateMode.EPHEMERAL);

			return true;
		    } catch (KeeperException.NodeExistsException e) {
			// ignore it
		    } catch (KeeperException e) {
			throw e;
		    }
		else
		    return false;
	    } else {
		try {
		    this.keeper.create(this.znode, ManagementFactory.getRuntimeMXBean().getName().getBytes(), this.acl,
			    CreateMode.EPHEMERAL);

		    return true;
		} catch (KeeperException.NodeExistsException e) {
		    // ignore it
		} catch (KeeperException e) {
		    throw e;
		}
	    }
	} while (System.currentTimeMillis() < last);

	return false;
    }

    /** {@inheritDoc} */
    @Override
    public void lock() {
	// sleep until the client is unlocked. if interrupted, eat the exception
	try {
	    this.lockIt(Long.MAX_VALUE, TimeUnit.DAYS);
	} catch (KeeperException e) {
	    throw new RuntimeException("KeeperException caught", e);
	} catch (InterruptedException e) {
	    // eat it
	}
    }

    /** {@inheritDoc} */
    @Override
    public void lockInterruptibly() throws InterruptedException {
	// sleep until the client is unlocked. if interrupted, just pass the
	// exception
	try {
	    this.lockIt(Long.MAX_VALUE, TimeUnit.DAYS);
	} catch (KeeperException e) {
	    throw new RuntimeException("KeeperException caught", e);
	}
    }

    /** {@inheritDoc} */
    @Override
    public Condition newCondition() {
	// we don't support conditions.... yet?
	throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public boolean tryLock() {
	try {
	    return this.lockIt(0, TimeUnit.MICROSECONDS);
	} catch (KeeperException e) {
	    throw new RuntimeException("KeeperException caught", e);
	} catch (InterruptedException e) {
	    throw new RuntimeException("InterruptedException caught", e);
	}
    }

    /** {@inheritDoc} */
    @Override
    public boolean tryLock(long t, TimeUnit tu) throws InterruptedException {
	try {
	    return this.lockIt(t, tu);
	} catch (KeeperException e) {
	    throw new RuntimeException("KeeperException caught", e);
	}
    }

    /** {@inheritDoc} */
    @Override
    public void unlock() {
	// check if we are already locked, throw an exception if so
	try {
	    Stat stat;
	    if ((stat = this.keeper.exists(this.znode, false)) == null)
		throw new IllegalStateException("already unlocked");

	    this.keeper.delete(this.znode, stat.getVersion());
	} catch (KeeperException e) {
	    throw new RuntimeException("KeeperException caught", e);
	} catch (InterruptedException e) {
	    throw new RuntimeException("InterruptedException caught", e);
	}
    }
}
