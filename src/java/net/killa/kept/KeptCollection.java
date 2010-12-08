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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * A Java {@link Collection} that is kept synchronized amongst a
 * {@link ZooKeeper} cluster.
 * 
 * NB: set updates are performed asynchronously via a {@link Watcher}, so there
 * may be a delay between modifying the list and it reflecting the change.
 * 
 */
public class KeptCollection implements Collection<String>, Synchronizable {
	private final SynchronizingWatcher watcher;
	protected final ArrayList<String> elements;

	private final ZooKeeper keeper;
	private final String znode;
	private final List<ACL> acl;
	private final CreateMode createMode;

	/**
	 * Construct a KeptCollection.
	 * 
	 * @param keeper
	 *            A {@link ZooKeeper} that is synchronized with
	 * 
	 * @param znode
	 *            A {@link String} containing the znode whose children will be
	 *            members of the collection
	 * 
	 * @param acl
	 *            A {@link List} of {@link ACL} containing the access control
	 *            lists for child node creation
	 * 
	 * @param createMode
	 *            A {@link CreateMode}, representing the mode of created
	 *            children.
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 * 
	 */
	public KeptCollection(ZooKeeper keeper, String znode, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
		this.elements = new ArrayList<String>();

		this.keeper = keeper;

		// if the znode doesn't exist, create a permanent znode with that path
		// TODO: change to allow ephemeral znode when ephemeral parents are
		// supported by zookeeper
		try {
			if (this.keeper.exists(znode, false) == null)
				this.keeper.create(znode, new byte[0], acl, CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException e) {
			// ignore this exception
		}

		this.znode = znode;
		this.acl = acl;
		if (createMode == CreateMode.PERSISTENT_SEQUENTIAL || createMode == CreateMode.EPHEMERAL_SEQUENTIAL)
			this.createMode = createMode;
		else if (createMode == CreateMode.PERSISTENT)
			this.createMode = CreateMode.PERSISTENT_SEQUENTIAL;
		else if (createMode == CreateMode.EPHEMERAL)
			this.createMode = CreateMode.EPHEMERAL_SEQUENTIAL;
		else
			throw new InvalidParameterException("unexpected create mode " + createMode.toString());

		this.watcher = new SynchronizingWatcher(this);

		this.synchronize();
	}

	/** {@inheritDoc} */
	@Override
	public void synchronize() throws KeeperException, InterruptedException {
		synchronized (this.elements) {
			try {
				// clear out the cache and reload it
				this.elements.clear();

				for (String s : this.keeper.getChildren(this.znode, this.watcher))
					this.elements.add(new String(this.keeper.getData(this.znode + '/' + s, false, null)));
			} catch (KeeperException.SessionExpiredException e) {
				// ignore it
			}
		}
	}

	protected boolean addUnsynchronized(String s) throws KeeperException, InterruptedException {
		this.keeper.create(this.znode + "/entry-", s.getBytes(), this.acl, this.createMode);

		return true;
	}

	protected boolean removeUnsynchronized(Object o) throws InterruptedException, KeeperException {
		String string = o.toString();

		for (String s : this.keeper.getChildren(this.znode, this.watcher))
			if (Arrays.equals(this.keeper.getData(this.znode + '/' + s, false, null), string.getBytes())) {
				this.keeper.delete(this.znode + '/' + s, -1);

				return true;
			}

		return false;
	}

	/**
	 * {@inheritDoc} NB: Nulls cannot be represented by this collection.
	 * Attempting to add one will cause an {@link IllegalArgumentException} to
	 * be thrown.
	 */
	@Override
	public boolean add(String o) {
		if (o == null)
			throw new IllegalArgumentException("nulls not allowed");

		try {
			return this.addUnsynchronized(o);
		} catch (KeeperException e) {
			throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
		}
	}

	/**
	 * {@inheritDoc} NB: Nulls cannot be represented by this collection.
	 * Attempting to add one will cause an {@link IllegalArgumentException} to
	 * be thrown.
	 */
	@Override
	public boolean addAll(Collection<? extends String> c) {
		boolean modified = false;

		for (String s : c)
			try {
				if (s == null)
					throw new IllegalArgumentException("nulls not allowed");

				if (this.addUnsynchronized(s))
					modified = true;
			} catch (KeeperException e) {
				throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
			}

		return modified;
	}

	/** {@inheritDoc} */
	@Override
	public void clear() {
		synchronized (this.elements) {
			try {
				for (String s : this.keeper.getChildren(this.znode, this.watcher))
					this.keeper.delete(this.znode + '/' + s, -1);

				this.synchronize();
			} catch (KeeperException e) {
				throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean contains(Object o) {
		return this.elements.contains(o);
	}

	/** {@inheritDoc} */
	@Override
	public boolean containsAll(Collection<?> c) {
		synchronized (this.elements) {
			for (Object o : c)
				if (!this.elements.contains(o))
					return false;

			return true;
		}

	}

	/** {@inheritDoc} */
	@Override
	public boolean isEmpty() {
		return this.elements.isEmpty();
	}

	/** {@inheritDoc} */
	@Override
	public Iterator<String> iterator() {
		return this.elements.iterator();
	}

	/** {@inheritDoc} */
	@Override
	public boolean remove(Object o) {
		try {
			return this.removeUnsynchronized(o);
		} catch (InterruptedException e) {
			throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
		} catch (KeeperException e) {
			throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean removeAll(Collection<?> c) {
		synchronized (this.elements) {
			try {
				boolean modified = false;

				for (Object o : c)
					if (this.removeUnsynchronized(o))
						modified = true;

				return modified;
			} catch (InterruptedException e) {
				throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
			} catch (KeeperException e) {
				throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean retainAll(Collection<?> c) {
		synchronized (this.elements) {
			try {
				// try not to copy unless necessary
				Set<? extends Object> thatset;
				if (c instanceof Set<?>)
					thatset = (Set<? extends Object>) c;
				else
					thatset = new HashSet<Object>(c);

				boolean changed = false;

				for (Object o : this.elements)
					if (!thatset.contains(o) && this.removeUnsynchronized(o) && !changed)
						changed = true;

				return changed;
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
		return this.elements.size();
	}

	/** {@inheritDoc} */
	@Override
	public Object[] toArray() {
		return this.elements.toArray();
	}

	/** {@inheritDoc} */
	@Override
	public <T> T[] toArray(T[] a) {
		return this.elements.toArray(a);
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		return this.elements.toString();
	}
}