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

import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

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
public class KeptSortedSet extends KeptSet implements SortedSet<String> {
    
    /**
     * Construct a KeptSortedSet.
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
    public KeptSortedSet(final ZooKeeper keeper, final String znode,
    	    final List<ACL> acl, final CreateMode createMode)
    	    throws KeeperException, InterruptedException {
    	
    	super(keeper, znode, acl, createMode, new TreeSet<String>());
    	
    }
    
	@Override
	public Comparator<? super String> comparator() {
		return  ((TreeSet<String>) this.set).comparator();
	}

	@Override
	public SortedSet<String> subSet(String fromElement, String toElement) {
		return ((TreeSet<String>) this.set).subSet(fromElement, toElement);
	}

	@Override
	public SortedSet<String> headSet(String toElement) {
		return ((TreeSet<String>) this.set).headSet(toElement);
	}

	@Override
	public SortedSet<String> tailSet(String fromElement) {
		return ((TreeSet<String>) this.set).tailSet(fromElement);
	}

	@Override
	public String first() {
		return (String) ((TreeSet<String>) this.set).first();
	}

	@Override
	public String last() {
		return (String) ((TreeSet<String>) this.set).last();
	}

}
