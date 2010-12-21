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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * A Java {@link BlockingQueue} that is kept synchronized amongst a
 * {@link ZooKeeper} cluster.
 * 
 * NB: set updates are performed asynchronously via a {@link Watcher}, so there
 * may be a delay between modifying the queue and it reflecting the change.
 * 
 */
public class KeptBlockingQueue<T> extends KeptQueue<T> implements
	BlockingQueue<T> {
    /**
     * Construct a KeptBlockingQueue.
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
    public KeptBlockingQueue(final Class<? extends T> elementClass,
	    final ZooKeeper keeper, final String znode, final List<ACL> acl,
	    final CreateMode mode) throws KeeperException, InterruptedException {
	super(elementClass, keeper, znode, acl, mode);
    }

    /** {@inheritDoc} */
    @Override
    public int drainTo(Collection<? super T> c) {
	return this.drainTo(c, Integer.MAX_VALUE);
    }

    /** {@inheritDoc} */
    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
	synchronized (this.elements) {
	    int size = Math.min(this.size(), maxElements);

	    c.addAll(this.subList(0, size));

	    try {
		for (int i = 0; i < size; i++)
		    this.removeUnsynchronized(i);
	    } catch (InterruptedException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (KeeperException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    }

	    return size;
	}
    }

    /** {@inheritDoc} */
    @Override
    public boolean offer(T o, long timeout, TimeUnit unit)
	    throws InterruptedException {
	// no capacity limitations for this queue (yet)
	return this.offer(o);
    }

    /** {@inheritDoc} */
    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
	long last = System.currentTimeMillis()
		+ TimeUnit.MILLISECONDS.convert(timeout, unit);

	do {
	    T element = this.poll();

	    if (element != null)
		return element;

	    Thread.sleep(100);
	} while (System.currentTimeMillis() <= last);

	return null;
    }

    /** {@inheritDoc} */
    @Override
    public void put(T o) throws InterruptedException {
	this.offer(o);
    }

    /** {@inheritDoc} */
    @Override
    public int remainingCapacity() {
	return Integer.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override
    public T take() throws InterruptedException {
	return this.poll(Long.MAX_VALUE, TimeUnit.DAYS);
    }
}
