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
package net.killa.unkempt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import net.killa.kept.KeptTestBase;
import net.killa.kept.Transformer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class UnkemptCollectionTest extends KeptTestBase {
    @Ignore
    @Test
    public void testUnkemptCollection() throws IOException, KeeperException,
	    InterruptedException {
	final String parent = this.getParent();

	final UnkemptCollection<String> kc = new UnkemptCollection<String>(
		String.class, this.keeper, parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	// check to see that changes made to the collection are reflected in the
	// znode
	String payload = Long.toString(System.currentTimeMillis());
	Assert.assertFalse(kc.contains(payload));

	kc.add(payload);

	String znode = null;
	for (final String node : this.keeper.getChildren(parent, false))
	    if (this.keeper.exists(parent + '/' + node, null) != null) {
		znode = node;
		break;
	    }
	Assert.assertNotNull("added entry does not exist in zookeeper", znode);

	this.keeper.delete(parent + '/' + znode, -1);

	Assert.assertFalse(kc.contains(payload));

	// check to see that changes on zookeeper are reflected in the
	// collection
	payload = Long.toString(System.currentTimeMillis());

	Assert.assertFalse(kc.contains(payload));

	final String fullPath = this.keeper.create(parent + "/node-",
		Transformer.objectToBytes(payload, String.class),
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	Assert.assertTrue(kc.contains(payload));

	Assert.assertTrue("not there", kc.remove(payload));

	Assert.assertNull("still there", this.keeper.exists(fullPath, null));

	Assert.assertFalse("still there", kc.remove(payload));
    }

    @Test
    public void testUnkemptCollectionClear() throws IOException,
	    KeeperException, InterruptedException {
	final UnkemptCollection<String> ks = new UnkemptCollection<String>(
		String.class, this.keeper, this.getParent(),
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	ks.add("one");
	ks.add("two");
	ks.add("three");

	Assert.assertEquals("not equal", 3, ks.size());

	ks.clear();

	Assert.assertTrue("collection is not empty", ks.isEmpty());

	Assert.assertEquals("collection is not empty", 0, ks.size());
    }

    @Ignore
    @Test
    public void testUnkemptCollectionAll() throws IOException, KeeperException,
	    InterruptedException {
	final Collection<String> hs = new ArrayList<String>();

	hs.add("one");
	hs.add("two");
	hs.add("three");

	final UnkemptCollection<String> s = new UnkemptCollection<String>(
		String.class, this.keeper, this.getParent(),
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	s.addAll(hs);

	Assert.assertEquals("not equal", 3, s.size());

	hs.add("four");

	Assert.assertEquals("not equal", 4, s.size());

	Assert.assertTrue("collection does not contain all", s.removeAll(hs));

	Assert.assertTrue("collection is not empty", s.isEmpty());
	Assert.assertEquals("collection is not empty", 0, s.size());
    }

    @Ignore
    @Test
    public void testUnkemptCollectionRetainAll() throws IOException,
	    KeeperException, InterruptedException {
	final Collection<String> al1 = new ArrayList<String>();

	al1.add("one");
	al1.add("two");
	al1.add("three");

	final Collection<String> al2 = new ArrayList<String>();

	al2.add("two");
	al2.add("three");
	al2.add("four");

	final UnkemptCollection<String> kc = new UnkemptCollection<String>(
		String.class, this.keeper, this.getParent(),
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	kc.addAll(al1);

	kc.retainAll(al2);
	al1.retainAll(al2);

	Assert.assertTrue("collection does not contain all", kc.contains("two"));
	Assert.assertTrue("collection does not contain all",
		kc.contains("three"));
	Assert.assertEquals("collection is the wrong size", 2, kc.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnkemptCollectionAddNull() throws IOException,
	    KeeperException, InterruptedException {
	final UnkemptCollection<Object> kc = new UnkemptCollection<Object>(
		String.class, this.keeper, this.getParent(),
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	kc.add(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnkemptCollectionAddAllNull() throws IOException,
	    KeeperException, InterruptedException {
	final UnkemptCollection<String> kc = new UnkemptCollection<String>(
		String.class, this.keeper, this.getParent(),
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	kc.addAll(Arrays.asList(new String[] { null }));
    }

    @Test
    public void testUnkemptCollectionIterator() throws KeeperException,
	    InterruptedException, IOException, ClassNotFoundException {
	final UnkemptCollection<String> ks = new UnkemptCollection<String>(
		String.class, this.keeper, this.getParent(),
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	ks.add("one");
	ks.add("two");
	ks.add("three");

	Assert.assertFalse("collection is empty", ks.isEmpty());

	final Iterator<String> it = ks.iterator();
	Assert.assertEquals("not equal", "one", it.next());
	Assert.assertEquals("not equal", "two", it.next());
	Assert.assertEquals("not equal", "three", it.next());
    }

    @Override
    public String getParent() {
	return "/testunkemptcollection";
    }
}
