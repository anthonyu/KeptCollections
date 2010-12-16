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

import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * A Java {@link Map} that is kept synchronized amongst a {@link ZooKeeper}
 * cluster.
 * 
 * NB: set updates are performed asynchronously via a {@link Watcher}, so there
 * may be a delay between modifying the keys or values and the map reflecting
 * the change.
 * 
 */
public class KeptMap implements Map<String, String>, Synchronizable {
    private static final Logger LOG = Logger.getLogger(KeptSet.class);

    private final SynchronizingWatcher watcher;
    protected final Map<String, String> map;

    private final ZooKeeper keeper;
    private final String znode;
    private final CreateMode createMode;
    private final List<ACL> acl;

    /**
     * Construct a KeptMap.
     * 
     * @param keeper
     *            A {@link ZooKeeper} that is synchronized with
     * 
     * @param znode
     *            A {@link String} containing the znode whose children will be
     *            members of the set
     * 
     * @param acl
     *            A {@link List} of {@link ACL} containing the access control
     *            lists for child node creation
     * 
     * @param createMode
     *            A {@link CreateMode} representing the persistence of created
     *            child nodes
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    public KeptMap(ZooKeeper keeper, String znode, List<ACL> acl,
	    CreateMode createMode) throws KeeperException, InterruptedException {
	this.map = new HashMap<String, String>();

	this.keeper = keeper;

	// if the znode doesn't exist, create a permanent znode with that path
	// TODO: change to allow ephemeral znode when ephemeral parents are
	// supported by zookeeper
	try {
	    if (this.keeper.exists(znode, false) == null)
		this.keeper.create(znode, new byte[0], acl,
			CreateMode.PERSISTENT);
	} catch (KeeperException.NodeExistsException e) {
	    KeptMap.LOG.debug("skipping creation of znode " + znode
		    + " as it already exists");
	}

	this.znode = znode;
	this.acl = acl;
	if (createMode == CreateMode.PERSISTENT
		|| createMode == CreateMode.EPHEMERAL)
	    this.createMode = createMode;
	else if (createMode == CreateMode.PERSISTENT_SEQUENTIAL)
	    this.createMode = CreateMode.PERSISTENT;
	else if (createMode == CreateMode.EPHEMERAL_SEQUENTIAL)
	    this.createMode = CreateMode.EPHEMERAL;
	else
	    throw new InvalidParameterException("unexpected create mode "
		    + createMode.toString());

	this.watcher = new SynchronizingWatcher(this);

	this.synchronize();
    }

    /** {@inheritDoc} */
    @Override
    public void synchronize() throws KeeperException, InterruptedException {
	synchronized (this.map) {
	    try {
		// empty out the cache
		this.map.clear();

		// then add all of the child nodes to the cache
		for (String s : this.keeper.getChildren(this.znode,
			this.watcher))
		    this.map.put(
			    s,
			    new String(this.keeper.getData(
				    this.znode + '/' + s, this.watcher, null)));
	    } catch (KeeperException.SessionExpiredException e) {
		// ignore it
	    }
	}
    }

    private String putUnsynchronized(String key, String value)
	    throws KeeperException, InterruptedException {
	// FIXME: support slashes in keys
	if (key.indexOf('/') >= 0)
	    throw new UnsupportedOperationException(
		    "no slashes allowed in keys");

	String path = this.znode + '/' + key;

	try {
	    this.keeper.create(path, value.getBytes(), this.acl,
		    this.createMode);

	    return null;
	} catch (KeeperException.NodeExistsException e) {
	    // it already exists
	    try {
		Stat stat = new Stat();
		int j = 0;

		while (true) {
		    // get the old value and its version
		    byte[] oldval = this.keeper.getData(path, false, stat);

		    try {
			// set the new value
			this.keeper.setData(path, value.getBytes(),
				stat.getVersion());

			// return the old value
			return new String(oldval);
		    } catch (KeeperException.BadVersionException f) {
			if (j > 10)
			    throw f;

			// someone updated it in between, try again
			KeptMap.LOG
				.debug("caught bad version attempting to update, retrying");

			Thread.sleep(50);
		    }
		}
	    } catch (KeeperException f) {
		throw new RuntimeException("KeeperException caught", f);
	    } catch (InterruptedException f) {
		// someone interrupted us. we need to let go
		throw new RuntimeException("InterruptedException caught", f);
	    }
	}
    }

    private String removeUnsynchronized(Object key)
	    throws InterruptedException, KeeperException {
	String path = this.znode + '/' + key;

	try {
	    Stat stat = new Stat();
	    int i = 0;

	    while (true) {
		try {
		    byte[] oldval = this.keeper.getData(path, false, stat);

		    this.keeper.delete(path, stat.getVersion());

		    return new String(oldval);
		} catch (KeeperException.BadVersionException e) {
		    if (i > 10)
			throw e;

		    // someone updated it in between, try again
		    KeptMap.LOG
			    .debug("caught bad version attempting to update, retrying");

		    Thread.sleep(50);
		}
	    }
	} catch (KeeperException.NoNodeException e) {
	    return null;
	}
    }

    /**
     * {@inheritDoc}
     * 
     * NB: clear is performed asynchronously, so there may be a small delay
     * before the map is empty.
     */
    @Override
    public void clear() {
	synchronized (this.map) {
	    try {
		for (String s : this.keeper.getChildren(this.znode,
			this.watcher))
		    this.keeper.delete(this.znode + '/' + s, -1);
	    } catch (KeeperException e) {
		throw new RuntimeException("KeeperException caught", e);
	    } catch (InterruptedException e) {
		throw new RuntimeException("InterruptedException caught", e);
	    }
	}
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(Object key) {
	return this.map.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsValue(Object value) {
	return this.map.containsValue(value);
    }

    /** {@inheritDoc} */
    @Override
    public Set<java.util.Map.Entry<String, String>> entrySet() {
	return this.map.entrySet();
    }

    /** {@inheritDoc} */
    @Override
    public String get(Object key) {
	return this.map.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEmpty() {
	return this.map.isEmpty();
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> keySet() {
	return this.map.keySet();
    }

    /**
     * {@inheritDoc}
     * 
     * NB: put is performed asynchronously, so there may be a small delay before
     * containsKey() will return true for the added key.
     */
    @Override
    public String put(String key, String value) {
	synchronized (this.map) {
	    try {
		return this.putUnsynchronized(key, value);
	    } catch (KeeperException e) {
		throw new RuntimeException("KeeperException caught", e);
	    } catch (InterruptedException e) {
		throw new RuntimeException("InterruptedException caught", e);
	    }
	}
    }

    /**
     * {@inheritDoc}
     * 
     * NB: put is performed asynchronously, so there may be a small delay before
     * containsKey() will return true for the added key.
     */
    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
	synchronized (this.map) {
	    try {
		for (Entry<? extends String, ? extends String> entry : m
			.entrySet())
		    this.putUnsynchronized(entry.getKey(), entry.getValue());
	    } catch (KeeperException e) {
		throw new RuntimeException("KeeperException caught", e);
	    } catch (InterruptedException e) {
		throw new RuntimeException("InterruptedException caught", e);
	    }
	}
    }

    /**
     * {@inheritDoc}
     * 
     * NB: remove is performed asynchronously, so there may be a small delay
     * before containsKey() will return false for the removed key.
     */
    @Override
    public String remove(Object key) {
	synchronized (this.map) {
	    try {
		return this.removeUnsynchronized(key);
	    } catch (KeeperException e) {
		throw new RuntimeException("KeeperException caught", e);
	    } catch (InterruptedException e) {
		throw new RuntimeException("InterruptedException caught", e);
	    }
	}
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
	return this.map.size();
    }

    /** {@inheritDoc} */
    @Override
    public Collection<String> values() {
	return this.map.values();
    }
}
