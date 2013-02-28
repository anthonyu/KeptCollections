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
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * A Java {@link Queue} that is kept synchronized amongst a {@link ZooKeeper}
 * cluster.
 * 
 * NB: set updates are performed asynchronously via a {@link Watcher}, so there
 * may be a delay between modifying the queue and it reflecting the change.
 * 
 */
public class KeptQueue<T> extends KeptList<T> implements Queue<T> {
    /**
     * Construct a KeptQueue.
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
    public KeptQueue(final Class<? extends T> elementClass,
	    final ZooKeeper keeper, final String znode, final List<ACL> acl,
	    final CreateMode mode) throws KeeperException, InterruptedException {
	super(elementClass, keeper, znode, acl, mode);
    }

    /** {@inheritDoc} */
    @Override
    public T element() {
	if (this.size() == 0)
	    throw new NoSuchElementException();

	return this.get(0);
    }

    /** {@inheritDoc} */
    @Override
    public boolean offer(T o) {
	return this.add(o);
    }

    /** {@inheritDoc} */
    @Override
    public T peek() {
	if (this.size() == 0)
	    return null;

	return this.get(0);
    }

    /** {@inheritDoc} */
    @Override
    public T poll() {
	if (this.size() == 0)
	    return null;

	return this.remove(0);
    }

    /** {@inheritDoc} */
    @Override
    public T remove() {
	if (this.size() == 0)
	    throw new NoSuchElementException();

	return this.remove(0);
    }
}
