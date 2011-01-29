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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Test;

public class TestKeptCollection extends BaseKeptUtil {
    {
	this.parent = "/testkeptcollection";
    }

    @Test
    public void testKeptCollection() throws IOException, KeeperException,
	    InterruptedException {
	KeptCollection<String> kc = new KeptCollection<String>(String.class,
		this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	// check to see that changes made to the collection are reflected in the
	// znode
	String payload = Long.toString(System.currentTimeMillis());
	Assert.assertFalse(kc.contains(payload));

	kc.add(payload);

	String znode = null;
	for (String node : this.keeper.getChildren(this.parent, false)) {
	    if (this.keeper.exists(this.parent + '/' + node, null) != null) {
		znode = node;
		break;
	    }
	}
	Assert.assertNotNull("added entry does not exist in zookeeper", znode);

	this.keeper.delete(this.parent + '/' + znode, -1);

	// wait for it to take effect
	Thread.sleep(100);

	Assert.assertFalse(kc.contains(payload));

	// check to see that changes on zookeeper are reflected in the
	// collection
	payload = Long.toString(System.currentTimeMillis());

	Assert.assertFalse(kc.contains(payload));

	String fullPath = this.keeper.create(this.parent + "/node-",
		Transformer.objectToBytes(payload, String.class),
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	// wait for it to take effect
	Thread.sleep(100);

	Assert.assertTrue(kc.contains(payload));

	Assert.assertTrue("not there", kc.remove(payload));

	Thread.sleep(100);

	Assert.assertNull("still there", this.keeper.exists(fullPath, null));

	Assert.assertFalse("still there", kc.remove(payload));
    }

    @Test
    public void testKeptCollectionClear() throws IOException, KeeperException,
	    InterruptedException {
	KeptCollection<String> ks = new KeptCollection<String>(String.class,
		this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	ks.add("one");
	ks.add("two");
	ks.add("three");

	// wait for it to take effect
	Thread.sleep(100);

	Assert.assertTrue("collection does not contain one", ks.contains("one"));
	Assert.assertTrue("collection does not contain two", ks.contains("two"));
	Assert.assertTrue("collection does not contain three",
		ks.contains("three"));

	ks.clear();

	// wait for it to take effect
	Thread.sleep(100);

	Assert.assertTrue("collection is not empty", ks.isEmpty());

	Assert.assertEquals("collection is not empty", 0, ks.size());
    }

    @Test
    public void testKeptCollectionAll() throws IOException, KeeperException,
	    InterruptedException {
	Collection<String> hs = new ArrayList<String>();

	hs.add("one");
	hs.add("two");
	hs.add("three");

	KeptCollection<String> s = new KeptCollection<String>(String.class,
		this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	s.addAll(hs);

	// wait for it to take effect
	Thread.sleep(100);

	Assert.assertTrue("collection does not contain all", s.containsAll(hs));

	hs.add("four");

	Assert.assertFalse("collection contains all", s.containsAll(hs));

	Assert.assertTrue("collection does not contain all", s.removeAll(hs));

	Thread.sleep(100);

	Assert.assertTrue("collection is not empty", s.isEmpty());
	Assert.assertEquals("collection is not empty", 0, s.size());
    }

    @Test
    public void testKeptCollectionRetainAll() throws IOException,
	    KeeperException, InterruptedException {
	Collection<String> al1 = new ArrayList<String>();

	al1.add("one");
	al1.add("two");
	al1.add("three");

	Collection<String> al2 = new ArrayList<String>();

	al2.add("two");
	al2.add("three");
	al2.add("four");

	KeptCollection<String> kc = new KeptCollection<String>(String.class,
		this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	kc.addAll(al1);

	// wait for it to take effect
	Thread.sleep(100);

	kc.retainAll(al2);
	al1.retainAll(al2);

	// wait for it to take effect
	Thread.sleep(100);

	Assert.assertTrue("collection does not contain all", kc.contains("two"));
	Assert.assertTrue("collection does not contain all",
		kc.contains("three"));
	Assert.assertEquals("collection is the wrong size", 2, kc.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeptCollectionAddNull() throws IOException,
	    KeeperException, InterruptedException {
	KeptCollection<Object> kc = new KeptCollection<Object>(String.class,
		this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	kc.add(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeptCollectionAddAllNull() throws IOException,
	    KeeperException, InterruptedException {
	KeptCollection<String> kc = new KeptCollection<String>(String.class,
		this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	kc.addAll(Arrays.asList(new String[] { null }));
    }

    @Test
    public void testKeptCollectionIterator() throws IOException,
	    KeeperException, InterruptedException {
	KeptCollection<String> ks = new KeptCollection<String>(String.class,
		this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	ks.add("one");
	ks.add("two");
	ks.add("three");

	// wait for it to take effect
	Thread.sleep(100);

	Assert.assertTrue("collection does not contain one", ks.contains("one"));
	Assert.assertTrue("collection does not contain two", ks.contains("two"));
	Assert.assertTrue("collection does not contain three",
		ks.contains("three"));

	for (Iterator<String> it = ks.iterator(); it.hasNext();)
	    if (it.next().equals("two"))
		it.remove();

	// wait for it to take effect
	Thread.sleep(100);

	Assert.assertTrue("collection does not contain one", ks.contains("one"));
	Assert.assertFalse("collection does contain two", ks.contains("two"));
	Assert.assertTrue("collection does not contain three",
		ks.contains("three"));
    }
}
