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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * A Java {@link List} that is kept synchronized amongst a {@link ZooKeeper}
 * cluster.
 * 
 * NB: set updates are performed asynchronously via a {@link Watcher}, so there
 * may be a delay between modifying the list and it reflecting the change.
 * 
 */
public class KeptList extends KeptCollection implements List<String>, Synchronizable {
	private final SynchronizingWatcher watcher;

	private final String znode;
	private final ZooKeeper keeper;

	private final ArrayList<String> indices;

	/**
	 * Construct a KeptList.
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
	 * @param mode
	 *            A {@link CreateMode} representing the persistence of created
	 *            child nodes
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public KeptList(ZooKeeper keeper, String znode, List<ACL> acl, CreateMode mode) throws KeeperException, InterruptedException {
		super(keeper, znode, acl, mode);

		this.znode = znode;
		this.keeper = keeper;

		this.indices = new ArrayList<String>();

		this.watcher = new SynchronizingWatcher(this);

		this.synchronize();
	}

	@Override
	public void synchronize() throws KeeperException, InterruptedException {
		if (this.indices != null)
			synchronized (this.elements) {
				try {
					// clear out the cache and reload it
					this.indices.clear();
					this.elements.clear();

					List<String> children = this.keeper.getChildren(this.znode, this.watcher);

					Collections.sort(children);

					for (String s : children) {
						this.indices.add((this.znode + '/' + s));
						this.elements.add(new String(this.keeper.getData(this.znode + '/' + s, this.watcher, null)));
					}
				} catch (KeeperException.SessionExpiredException e) {
					// ignore it
				}
			}
	}

	protected boolean removeUnsynchronized(int i) throws InterruptedException, KeeperException {
		this.keeper.delete(this.indices.get(i), -1);

		return true;
	}

	/** Not supported */
	@Override
	public void add(int index, String element) {
		throw new UnsupportedOperationException();
	}

	/** Not supported */
	@Override
	public boolean addAll(int index, Collection<? extends String> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String get(int index) {
		if (index >= this.size())
			throw new IndexOutOfBoundsException(index + " >= " + this.size());

		return this.elements.get(index);
	}

	@Override
	public int indexOf(Object o) {
		return this.elements.indexOf(o);
	}

	@Override
	public int lastIndexOf(Object o) {
		return this.elements.lastIndexOf(o);
	}

	@Override
	public ListIterator<String> listIterator() {
		return this.elements.listIterator();
	}

	@Override
	public ListIterator<String> listIterator(int index) {
		return this.elements.listIterator(index);
	}

	@Override
	public String remove(int index) {
		synchronized (this.elements) {
			if (index >= this.size())
				throw new IndexOutOfBoundsException(index + " >= " + this.size());

			try {
				String s = new String(this.keeper.getData(this.indices.get(index), false, null));
				this.removeUnsynchronized(index);
				return s;
			} catch (InterruptedException e) {
				throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
			} catch (KeeperException e) {
				throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
			}
		}
	}

	@Override
	public String set(int index, String element) {
		if (index >= this.size())
			throw new IndexOutOfBoundsException(index + " >= " + this.size());

		if (element == null)
			throw new IllegalArgumentException("nulls not allowed");

		try {
			String path = this.indices.get(index);
			String s = new String(this.keeper.getData(path, false, null));
			this.keeper.setData(path, element.getBytes(), -1);
			return s;
		} catch (KeeperException e) {
			throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e.getClass().getSimpleName() + " caught", e);
		}
	}

	@Override
	public List<String> subList(int fromIndex, int toIndex) {
		return this.elements.subList(fromIndex, toIndex);
	}
}