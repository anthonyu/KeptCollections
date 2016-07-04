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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * A Java {@link ConcurrentMap} that is kept synchronized amongst a
 * {@link ZooKeeper} cluster.
 * 
 * NB: set updates are performed asynchronously via a {@link Watcher}, so there
 * may be a delay between modifying the keys or values and the map reflecting
 * the change.
 * 
 */
public class KeptConcurrentMap<V> extends KeptMap<V> implements
	ConcurrentMap<String, V>, Synchronizable {
    private final ZooKeeper keeper;
    private final String znode;
    private final List<ACL> acl;
    private final CreateMode createMode;

    /**
     * Construct a KeptConcurrentMap.
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
    public KeptConcurrentMap(final Class<? extends V> elementClass,
    		final ZooKeeper keeper, final String znode,
	    final List<ACL> acl, final CreateMode createMode)
	    throws KeeperException, InterruptedException {
	super(elementClass, keeper, znode, acl, createMode);

	this.keeper = keeper;
	this.znode = znode;
	this.acl = acl;
	this.createMode = createMode;
    }

    @Override
    public V putIfAbsent(final String key, final V value) {
	synchronized (this.map) {
	    try {
		this.keeper.create(this.znode + '/' + key, 
				Transformer.objectToBytes(value, this.elementClass),
			this.acl, this.createMode);

		return null;
	    } catch (final KeeperException.NodeExistsException e) {
		return this.get(key);
	    } catch (final KeeperException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (final InterruptedException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (IOException e) {
	    	throw new RuntimeException(e.getClass().getSimpleName()
	    			+ " caught", e);
		}
	}
    }

    @Override
    public boolean remove(final Object key, final Object value) {
	synchronized (this.map) {
	    try {
		final Stat stat = new Stat();

		final String string = new String(this.keeper.getData(this.znode
			+ '/' + key, true, stat));

		if (string.equals(value.toString())) {
		    this.keeper.delete(this.znode + '/' + key,
			    stat.getVersion());

		    return true;
		} else
		    return false;
	    } catch (final KeeperException.NoNodeException e) {
		return false;
	    } catch (final KeeperException.BadVersionException e) {
		return false;
	    } catch (final KeeperException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (final InterruptedException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    }
	}
    }

    @Override
    public V replace(final String key, final V value) {
	synchronized (this.map) {
	    try {
		@SuppressWarnings("unchecked")
		final V data = (V) Transformer.bytesToObject(this.keeper.getData(this.znode
			+ '/' + key, true, null), this.elementClass);

		this.keeper.setData(this.znode + '/' + key, 
				Transformer.objectToBytes(value, this.elementClass),
			-1);

		return data;
	    } catch (final KeeperException.NoNodeException e) {
		return null;
	    } catch (final KeeperException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (final InterruptedException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (IOException e) {
	    	throw new RuntimeException(e.getClass().getSimpleName()
	    			+ " caught", e);
		} catch (ClassNotFoundException e) {
	    	throw new RuntimeException(e.getClass().getSimpleName()
	    			+ " caught", e);
		}
	}
    }

    @Override
    public boolean replace(final String key, final V oldValue,
	    final V newValue) {
	synchronized (this.map) {
	    try {
		final Stat stat = new Stat();

		@SuppressWarnings("unchecked")
		final V data = (V) Transformer.bytesToObject(this.keeper.getData(this.znode
			+ '/' + key, true, stat), this.elementClass);

		if (data.equals(oldValue.toString())) {
		    this.keeper.setData(this.znode + '/' + key,
		    		Transformer.objectToBytes(newValue, this.elementClass), 
		    		stat.getVersion());

		    return true;
		} else
		    return false;
	    } catch (final KeeperException.NoNodeException e) {
		return false;
	    } catch (final KeeperException.BadVersionException e) {
		return false;
	    } catch (final KeeperException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (final InterruptedException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (final Exception e) {
			throw new RuntimeException(e.getClass().getSimpleName()
					+ " caught", e);
		}
	}
    }
}
