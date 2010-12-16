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
import java.util.HashSet;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Test;

public class TestKeptSet extends BaseKeptUtil {
    {
	this.parent = "/testkeptset";
    }

    @Test
    public void testKeptSet() throws IOException, KeeperException,
	    InterruptedException {
	KeptSet ks = new KeptSet(this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	// check to see that changes made to the set are reflected in the znode
	String znode = Long.toString(System.currentTimeMillis());

	Assert.assertFalse(ks.contains(znode));

	ks.add(znode);

	Assert.assertNotNull(this.keeper
		.exists(this.parent + '/' + znode, null));

	this.keeper.delete(this.parent + '/' + znode, -1);

	// wait for it to take effect
	Thread.sleep(50);

	Assert.assertFalse(ks.contains(znode));

	// check to see that changes on zookeeper are reflected in the set
	znode = Long.toString(System.currentTimeMillis());

	Assert.assertFalse(ks.contains(znode));

	this.keeper.create(this.parent + '/' + znode, new byte[0],
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	// wait for it to take effect
	Thread.sleep(50);

	Assert.assertTrue(ks.contains(znode));

	ks.remove(znode);

	Assert.assertNull(this.keeper.exists(this.parent + '/' + znode, null));
    }

    @Test
    public void testKeptSetReAdd() throws IOException, KeeperException,
	    InterruptedException {
	KeptSet ks = new KeptSet(this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	// check to see that changes made to the set are reflected in the znode
	String znode = Long.toString(System.currentTimeMillis());

	Assert.assertFalse(ks.contains(znode));

	Assert.assertTrue(ks.add(znode));
	Assert.assertFalse(ks.add(znode));

	Thread.sleep(50);

	Assert.assertFalse(ks.add(znode));
    }

    @Test
    public void testKeptSetClear() throws IOException, KeeperException,
	    InterruptedException {
	KeptSet ks = new KeptSet(this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	ks.add("one");
	ks.add("two");
	ks.add("three");

	// wait for it to take effect
	Thread.sleep(50);

	Assert.assertTrue("set does not contain one", ks.contains("one"));
	Assert.assertTrue("set does not contain two", ks.contains("two"));
	Assert.assertTrue("set does not contain three", ks.contains("three"));

	ks.clear();

	// wait for it to take effect
	Thread.sleep(50);

	Assert.assertTrue("set is not empty", ks.isEmpty());

	Assert.assertEquals("set is not empty", 0, ks.size());
    }

    @Test
    public void testKeptSetAddAll() throws IOException, KeeperException,
	    InterruptedException {
	Set<String> hs = new HashSet<String>();

	hs.add("one");
	hs.add("two");
	hs.add("three");

	KeptSet s = new KeptSet(this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	s.addAll(hs);

	// wait for it to take effect
	Thread.sleep(50);

	Assert.assertTrue("set does not contain all", s.containsAll(hs));
    }

    @Test
    public void testKeptSetRetainAll() throws IOException, KeeperException,
	    InterruptedException {
	Set<String> hs1 = new HashSet<String>();

	hs1.add("one");
	hs1.add("two");
	hs1.add("three");

	Set<String> hs2 = new HashSet<String>();

	hs2.add("two");
	hs2.add("three");
	hs2.add("four");

	KeptSet ks = new KeptSet(this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	ks.addAll(hs1);

	// wait for it to take effect
	Thread.sleep(50);

	ks.retainAll(hs2);
	hs1.retainAll(hs2);

	// wait for it to take effect
	Thread.sleep(50);

	Assert.assertTrue("set does not contain all", ks.containsAll(hs1));
	Assert.assertEquals("sets are not the same size", hs1.size(), ks.size());
    }
}
