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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Test;

public class TestKeptMap extends BaseKeptUtil {
	{
		this.parent = "/testkeptmap";
	}

	@Test
	public void testKeptMap() throws IOException, KeeperException, InterruptedException {
		KeptMap s = new KeptMap(this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		// check to see that changes made to the map are reflected in the znode
		String znode = Long.toString(System.currentTimeMillis());
		String value = "lorem ipsum";

		Assert.assertFalse(s.containsKey(znode));

		s.put(znode, value);

		Assert.assertNotNull(this.keeper.exists(this.parent + '/' + znode, null));

		this.keeper.delete(this.parent + '/' + znode, -1);

		// wait for it to take effect
		Thread.sleep(100);

		Assert.assertFalse(s.containsKey(znode));

		// check to see that changes on zookeeper are reflected in the map
		znode = Long.toString(System.currentTimeMillis());

		Assert.assertFalse(s.containsKey(znode));

		this.keeper.create(this.parent + '/' + znode, value.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		// wait for it to take effect
		Thread.sleep(100);

		Assert.assertTrue(s.containsKey(znode));
		Assert.assertEquals(value, s.get(znode));

		s.remove(znode);

		Assert.assertNull(this.keeper.exists(this.parent + '/' + znode, null));
	}

	@Test
	public void testKeptMapClear() throws IOException, KeeperException, InterruptedException {
		KeptMap km = new KeptMap(this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		km.put("one", "value");
		km.put("two", "value");
		km.put("three", "value");

		// wait for it to take effect
		Thread.sleep(100);

		Assert.assertTrue("map does not contain one", km.containsKey("one"));
		Assert.assertTrue("map does not contain two", km.containsKey("two"));
		Assert.assertTrue("map does not contain three", km.containsKey("three"));

		km.clear();

		// wait for it to take effect
		Thread.sleep(100);

		Assert.assertTrue("map is not empty", km.isEmpty());

		Assert.assertEquals("map is not empty", 0, km.size());
	}

	@Test
	public void testKeptMapOverwrite() throws KeeperException, InterruptedException {
		KeptMap km = new KeptMap(this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		Assert.assertNull("value not previously null", km.put("one", "value"));

		// wait for it to take effect
		Thread.sleep(100);

		Assert.assertEquals("map does not contain one", "value", km.get("one"));

		Assert.assertEquals("value not previously equal", "value", km.put("one", "eulav"));

		// wait for it to take effect
		Thread.sleep(100);

		Assert.assertEquals("map does not contain one", "eulav", km.get("one"));

		// try again, but run to win the race condition in synchronize
		Assert.assertNull("value not previously null", km.put("two", "value"));
		Assert.assertEquals("value not previously equal", "value", km.put("two", "eulav"));
	}
}
