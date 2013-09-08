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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * A Java {@link TreeSet} that is kept synchronized amongst a {@link ZooKeeper}
 * cluster.
 * 
 * NB: set updates are performed asynchronously via a {@link Watcher}, so there
 * may be a delay between modifying the set and the contents reflecting the
 * change.
 * 
 */
public class KeptTreeSet implements SortedSet<String>, Synchronizable {
	private static final Logger LOG = Logger.getLogger(KeptTreeSet.class);

    private final SynchronizingWatcher watcher;
    private final SortedSet<String> set;

    private final ZooKeeper keeper;
    private final String znode;
    private final CreateMode createMode;
    private final List<ACL> acl;
    
    /**
     * Construct a KeptTreeSet.
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
    public KeptTreeSet(final ZooKeeper keeper, final String znode,
    	    final List<ACL> acl, final CreateMode createMode)
    	    throws KeeperException, InterruptedException {
    	this.set = new TreeSet<String>();
    	this.keeper = keeper;
    	
    	// if the znode doesn't exist, create a permanent znode with that path
    	// TODO: change to allow ephemeral znode when ephemeral parents are
    	// supported by zookeeper
    	try {
    	    if (this.keeper.exists(znode, false) == null)
    	    	this.keeper.create(znode, new byte[0], acl,
    	    			CreateMode.PERSISTENT);
    	} catch (final KeeperException.NodeExistsException e) {
    		KeptTreeSet.LOG.debug("skipping creation of znode " + znode
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
    
    /**
     * Synchronize the TreeSet with the ZooKeeper backing store.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Override
	public void synchronize() throws KeeperException, InterruptedException {
    	synchronized (this.set) {
    	    try {
    	    	// clear out the cache and reload it
    	    	this.set.clear();

	    		for (final String s : this.keeper.getChildren(this.znode,
	    			this.watcher))
	    		    this.set.add(s);
	    	    } catch (final KeeperException.SessionExpiredException e) {
	    		// ignore it
    	    }
    	}
	}
    
    private boolean addUnsynchronized(final String s) throws KeeperException,
	    InterruptedException {
		// TODO: slashes is not allowed in keys
		if (s.indexOf('/') >= 0)
		    throw new UnsupportedOperationException(
			    "no slashes allowed in keys");
		
		if (this.set.contains(s))
		    return false;
		
		try {
		    this.keeper.create(this.znode + '/' + s, new byte[0], this.acl,
			    this.createMode);
		
		    return true;
		} catch (final KeeperException.NodeExistsException e) {
		    return false;
		}
	}
		
	private boolean removeUnsynchronized(final Object o)
		    throws InterruptedException, KeeperException {
		if (this.set.contains(o)) {
		    this.keeper.delete(this.znode + '/' + o, -1);
		
		    return true;
		}
		
		return false;
	}
	
	/**
     * {@inheritDoc}
     * 
     * NB: addition is performed asynchronously, so there may be a small delay
     * before contains() will return true for the added {@link String}.
     */
	@Override
	public boolean add(final String s) {
		synchronized (this.set) {
		    try {
		    	return this.addUnsynchronized(s);
		    } catch (final Exception e) {
				throw new RuntimeException(e.getClass().getSimpleName()
					+ " caught", e);
		    }
		}
	}
	
	/**
     * {@inheritDoc}
     * 
     * NB: addition is performed asynchronously, so there may be a small delay
     * before eventual consistency and contains() will return true for all of
     * the additions.
     */
    @Override
    public boolean addAll(final Collection<? extends String> c) {
    	synchronized (this.set) {
    	    try {
    	    	boolean changed = false;

	    		for (final String s : c)
	    		    if (this.addUnsynchronized(s) && !changed)
	    		    	changed = true;

				return changed;
    	    } catch (final Exception e) {
	    		throw new RuntimeException(e.getClass().getSimpleName()
	    			+ " caught", e);
    	    }
    	}
    }
    
    /** {@inheritDoc} */
    @Override
    public void clear() {
		synchronized (this.set) {
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
    public boolean contains(final Object o) {
    	return this.set.contains(o);
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean containsAll(final Collection<?> c) {
    	return this.set.containsAll(c);
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean isEmpty() {
    	return this.set.isEmpty();
    }
    
    /** {@inheritDoc} */
    @Override
    public Iterator<String> iterator() {
    	return this.set.iterator();
    }
    
    /**
     * {@inheritDoc}
     * 
     * NB: removal is performed asynchronously, so there may be a small delay
     * before eventual consistency and contains() will return false for the
     * deletion.
     */
    @Override
    public boolean remove(final Object o) {
		synchronized (this.set) {
		    try {
		    	return this.removeUnsynchronized(o);
		    } catch (final Exception e) {
		    	throw new RuntimeException(e.getClass().getSimpleName()
		    			+ " caught", e);
		    }
		}
    }
    
    /**
     * {@inheritDoc}
     * 
     * NB: removal is performed asynchronously, so there may be a small delay
     * before eventual consistency and contains() will return false for all of
     * the deletions.
     */
    @Override
    public boolean removeAll(final Collection<?> c) {
		synchronized (this.set) {
		    try {
				boolean changed = false;
				for (final Object o : c)
				    if (this.removeUnsynchronized(o) && !changed)
				    	changed = true;
		
				return changed;
		    } catch (final Exception e) {
				throw new RuntimeException(e.getClass().getSimpleName()
					+ " caught", e);
		    }
		}
    }
    
    /**
     * {@inheritDoc}
     * 
     * NB: removal is performed asynchronously, so there may be a small delay
     * before eventual consistency and contains() will return false for all of
     * the deletions.
     */
    @Override
    public boolean retainAll(final Collection<? extends Object> c) {
		synchronized (this.set) {
		    try {
		    	// try not to copy unless necessary
				Set<? extends Object> thatset;
				if (c instanceof Set<?>)
				    thatset = (Set<? extends Object>) c;
				else
				    thatset = new HashSet<Object>(c);
		
				boolean changed = false;
		
				for (final Object o : this.set)
				    if (!thatset.contains(o) && this.removeUnsynchronized(o)
					    && !changed)
				    	changed = true;
		
				return changed;
		    } catch (final Exception e) {
		    	throw new RuntimeException(e.getClass().getSimpleName()
		    			+ " caught", e);
		    }
		}
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
    	return this.set.size();
    }


    /** {@inheritDoc} */
    @Override
    public Object[] toArray() {
    	return this.set.toArray();
    }

    /** {@inheritDoc} */
    @Override
    public <T> T[] toArray(final T[] a) {
    	return this.set.toArray(a);
    }

    @Override
    public String toString() {
    	return this.set.toString();
    }

	@Override
	public Comparator<? super String> comparator() {
		return  this.set.comparator();
	}

	@Override
	public SortedSet<String> subSet(String fromElement, String toElement) {
		return this.set.subSet(fromElement, toElement);
	}

	@Override
	public SortedSet<String> headSet(String toElement) {
		return this.set.headSet(toElement);
	}

	@Override
	public SortedSet<String> tailSet(String fromElement) {
		return this.set.tailSet(fromElement);
	}

	@Override
	public String first() {
		return this.set.first();
	}

	@Override
	public String last() {
		return this.set.last();
	}

}
