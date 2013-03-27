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
    private static final Logger LOG = Logger.getLogger(KeptMap.class);

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
    public KeptMap(final ZooKeeper keeper, final String znode,
	    final List<ACL> acl, final CreateMode createMode)
	    throws KeeperException, InterruptedException {
	this.map = new HashMap<String, String>();

	this.keeper = keeper;

	// if the znode doesn't exist, create a permanent znode with that path
	// TODO: change to allow ephemeral znode when ephemeral parents are
	// supported by zookeeper
	try {
	    if (this.keeper.exists(znode, false) == null)
		this.keeper.create(znode, new byte[0], acl,
			CreateMode.PERSISTENT);
	} catch (final KeeperException.NodeExistsException e) {
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
		for (final String s : this.keeper.getChildren(this.znode,
			this.watcher))
		    this.map.put(
			    s,
			    new String(this.keeper.getData(
				    this.znode + '/' + s, this.watcher, null)));
	    } catch (final KeeperException.SessionExpiredException e) {
		// ignore it
	    }
	}
    }

    private String putUnsynchronized(final String key, final String value)
	    throws KeeperException, InterruptedException {
	// FIXME: support slashes in keys
	if (key.indexOf('/') >= 0)
	    throw new UnsupportedOperationException(
		    "no slashes allowed in keys");

	final String path = this.znode + '/' + key;

	try {
	    this.keeper.create(path, value.getBytes(), this.acl,
		    this.createMode);

	    return null;
	} catch (final KeeperException.NodeExistsException e) {
	    // it already exists
	    try {
		final Stat stat = new Stat();

		int j = 0;
		while (true) {
		    // get the old value and its version
		    final byte[] oldval = this.keeper
			    .getData(path, false, stat);

		    try {
			// set the new value
			this.keeper.setData(path, value.getBytes(),
				stat.getVersion());

			// return the old value
			return new String(oldval);
		    } catch (final KeeperException.BadVersionException f) {
			if (j++ > 9)
			    throw f;

			// someone updated it in between, try again
			KeptMap.LOG
				.debug("caught bad version attempting to update, retrying");

			Thread.sleep(50);
		    }
		}
	    } catch (final Exception f) {
		throw new RuntimeException(f.getClass().getSimpleName()
			+ " caught", f);
	    }
	}
    }

    private String removeUnsynchronized(final Object key)
	    throws InterruptedException, KeeperException {
	final String path = this.znode + '/' + key;

	try {
	    final Stat stat = new Stat();
	    int i = 0;

	    while (true)
		try {
		    final byte[] oldval = this.keeper
			    .getData(path, false, stat);

		    this.keeper.delete(path, stat.getVersion());

		    return new String(oldval);
		} catch (final KeeperException.BadVersionException e) {
		    i++;
		    if (i > 10)
			throw e;

		    // someone updated it in between, try again
		    KeptMap.LOG
			    .debug("caught bad version attempting to update, retrying");

		    Thread.sleep(50);
		}
	} catch (final KeeperException.NoNodeException e) {
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
		for (final String s : this.keeper.getChildren(this.znode,
			this.watcher))
		    this.keeper.delete(this.znode + '/' + s, -1);
	    } catch (final Exception e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    }
	}
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(final Object key) {
	return this.map.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsValue(final Object value) {
	return this.map.containsValue(value);
    }

    /** {@inheritDoc} */
    @Override
    public Set<java.util.Map.Entry<String, String>> entrySet() {
	return this.map.entrySet();
    }

    /** {@inheritDoc} */
    @Override
    public String get(final Object key) {
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
    public String put(final String key, final String value) {
	synchronized (this.map) {
	    try {
		return this.putUnsynchronized(key, value);
	    } catch (final Exception e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
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
    public void putAll(final Map<? extends String, ? extends String> m) {
	synchronized (this.map) {
	    try {
		for (final Entry<? extends String, ? extends String> entry : m
			.entrySet())
		    this.putUnsynchronized(entry.getKey(), entry.getValue());
	    } catch (final Exception e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
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
    public String remove(final Object key) {
	synchronized (this.map) {
	    try {
		return this.removeUnsynchronized(key);
	    } catch (final Exception e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
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
