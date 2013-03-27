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
public class KeptList<T> extends KeptCollection<T> implements List<T>,
	Synchronizable {
    private final SynchronizingWatcher watcher;

    private final String znode;
    private final ZooKeeper keeper;

    private final List<String> indices;

    /**
     * Construct a KeptList.
     * 
     * @param elementClass
     *            A {@link Class} representing the class of object that will be
     *            elements of this collection
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
    public KeptList(final Class<? extends T> elementClass,
	    final ZooKeeper keeper, final String znode, final List<ACL> acl,
	    final CreateMode mode) throws KeeperException, InterruptedException {
	super(elementClass, keeper, znode, acl, mode);

	this.znode = znode;
	this.keeper = keeper;

	this.indices = new ArrayList<String>();

	this.watcher = new SynchronizingWatcher(this);

	this.synchronize();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void synchronize() throws KeeperException, InterruptedException {
	if (this.indices != null)
	    synchronized (this.elements) {
		try {
		    // clear out the cache and reload it
		    this.indices.clear();
		    this.elements.clear();

		    final List<String> children = this.keeper.getChildren(
			    this.znode, this.watcher);

		    Collections.sort(children);

		    for (final String s : children) {
			this.indices.add(this.znode + '/' + s);
			this.elements
				.add((T) Transformer.bytesToObject(
					this.keeper.getData(this.znode + '/'
						+ s, this.watcher, null),
					this.elementClass));
		    }
		} catch (final KeeperException.SessionExpiredException e) {
		    throw new RuntimeException(e.getClass().getSimpleName()
			    + " caught", e);
		} catch (final ClassNotFoundException e) {
		    throw new RuntimeException(e.getClass().getSimpleName()
			    + " caught", e);
		} catch (final IOException e) {
		    throw new RuntimeException(e.getClass().getSimpleName()
			    + " caught", e);
		}
	    }
    }

    @Override
    protected boolean removeUnsynchronized(final int i)
	    throws InterruptedException, KeeperException {
	this.keeper.delete(this.indices.get(i), -1);

	return true;
    }

    /** Not supported */
    @Override
    public void add(final int index, final T element) {
	throw new UnsupportedOperationException();
    }

    /** Not supported */
    @Override
    public boolean addAll(final int index, final Collection<? extends T> c) {
	throw new UnsupportedOperationException();
    }

    @Override
    public T get(final int index) {
	if (index >= this.size())
	    throw new IndexOutOfBoundsException(index + " >= " + this.size());

	return this.elements.get(index);
    }

    @Override
    public int indexOf(final Object o) {
	return this.elements.indexOf(o);
    }

    @Override
    public int lastIndexOf(final Object o) {
	return this.elements.lastIndexOf(o);
    }

    @Override
    public ListIterator<T> listIterator() {
	return this.elements.listIterator();
    }

    @Override
    public ListIterator<T> listIterator(final int index) {
	return this.elements.listIterator(index);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T remove(final int index) {
	synchronized (this.elements) {
	    if (index >= this.size())
		throw new IndexOutOfBoundsException(index + " >= "
			+ this.size());

	    try {
		final T previous = (T) Transformer.bytesToObject(this.keeper
			.getData(this.indices.get(index), false, null),
			this.elementClass);
		this.removeUnsynchronized(index);
		return previous;
	    } catch (final InterruptedException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (final KeeperException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (final ClassNotFoundException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (final IOException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    }
	}
    }

    @Override
    public T set(final int index, final T element) {
	if (index >= this.size())
	    throw new IndexOutOfBoundsException(index + " >= " + this.size());

	if (element == null)
	    throw new IllegalArgumentException("nulls not allowed");

	try {
	    final String path = this.indices.get(index);
	    @SuppressWarnings("unchecked")
	    final T previous = (T) Transformer.bytesToObject(
		    this.keeper.getData(path, false, null), this.elementClass);
	    this.keeper.setData(path,
		    Transformer.objectToBytes(element, this.elementClass), -1);
	    return previous;
	} catch (final KeeperException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	} catch (final InterruptedException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	} catch (final ClassNotFoundException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	} catch (final IOException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	}
    }

    @Override
    public List<T> subList(final int fromIndex, final int toIndex) {
	return this.elements.subList(fromIndex, toIndex);
    }
}