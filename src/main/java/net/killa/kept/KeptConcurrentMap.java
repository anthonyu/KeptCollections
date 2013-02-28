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
public class KeptConcurrentMap extends KeptMap implements
	ConcurrentMap<String, String>, Synchronizable {
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
    public KeptConcurrentMap(ZooKeeper keeper, String znode, List<ACL> acl,
	    CreateMode createMode) throws KeeperException, InterruptedException {
	super(keeper, znode, acl, createMode);

	this.keeper = keeper;
	this.znode = znode;
	this.acl = acl;
	this.createMode = createMode;
    }

    @Override
    public String putIfAbsent(String key, String value) {
	synchronized (this.map) {
	    try {
		this.keeper.create(this.znode + '/' + key, value.getBytes(),
			this.acl, this.createMode);

		return null;
	    } catch (KeeperException.NodeExistsException e) {
		return this.get(key);
	    } catch (KeeperException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (InterruptedException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    }
	}
    }

    @Override
    public boolean remove(Object key, Object value) {
	synchronized (this.map) {
	    try {
		Stat stat = new Stat();

		String string = new String(this.keeper.getData(this.znode + '/'
			+ key, true, stat));

		if (string.equals(value.toString())) {
		    this.keeper.delete(this.znode + '/' + key,
			    stat.getVersion());

		    return true;
		} else {
		    return false;
		}
	    } catch (KeeperException.NoNodeException e) {
		return false;
	    } catch (KeeperException.BadVersionException e) {
		return false;
	    } catch (KeeperException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (InterruptedException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    }
	}
    }

    @Override
    public String replace(String key, String value) {
	synchronized (this.map) {
	    try {
		String data = new String(this.keeper.getData(this.znode + '/'
			+ key, true, null));

		this.keeper.setData(this.znode + '/' + key, value.getBytes(),
			-1);

		return data;
	    } catch (KeeperException.NoNodeException e) {
		return null;
	    } catch (KeeperException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (InterruptedException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    }
	}
    }

    @Override
    public boolean replace(String key, String oldValue, String newValue) {
	synchronized (this.map) {
	    try {
		Stat stat = new Stat();

		String data = new String(this.keeper.getData(this.znode + '/'
			+ key, true, stat));

		if (data.equals(oldValue.toString())) {
		    this.keeper.setData(this.znode + '/' + key,
			    newValue.getBytes(), stat.getVersion());

		    return true;
		} else {
		    return false;
		}
	    } catch (KeeperException.NoNodeException e) {
		return false;
	    } catch (KeeperException.BadVersionException e) {
		return false;
	    } catch (KeeperException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (InterruptedException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    }
	}
    }
}
