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
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestKeptQueue {
	private static final String PARENT = "/testkeptqueue";

	private ZooKeeper keeper;

	@Before
	public void before() throws IOException, InterruptedException, KeeperException {
		CountDownLatch latch = new CountDownLatch(1);

		// FIXME: set up a zookeeper server in process
		CountDownOnConnectWatcher watcher = new CountDownOnConnectWatcher();
		watcher.setLatch(latch);
		this.keeper = new ZooKeeper("localhost:2181", 20000, watcher);
		if (!latch.await(5, TimeUnit.SECONDS))
			throw new RuntimeException("unable to connect to server");
	}

	@After
	public void after() throws InterruptedException, KeeperException {
		for (String s : this.keeper.getChildren(TestKeptQueue.PARENT, false))
			this.keeper.delete(TestKeptQueue.PARENT + '/' + s, -1);
		this.keeper.close();
	}

	@Test
	public void testKeptQueue() throws IOException, KeeperException, InterruptedException {
		KeptQueue kq = new KeptQueue(this.keeper, TestKeptQueue.PARENT, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		String payload1 = Long.toString(System.currentTimeMillis());

		Thread.sleep(100);

		String payload2 = Long.toString(System.currentTimeMillis());

		Thread.sleep(100);

		String payload3 = Long.toString(System.currentTimeMillis());

		Assert.assertNull("not null", kq.peek());

		kq.offer(payload1);

		Thread.sleep(100);

		kq.offer(payload2);

		Thread.sleep(100);

		kq.offer(payload3);

		Thread.sleep(100);

		Assert.assertEquals("not equal", payload1, kq.peek());
		Assert.assertEquals("not equal", payload1, kq.remove());

		Thread.sleep(100);

		Assert.assertEquals("not equal", payload2, kq.element());
		Assert.assertEquals("not equal", payload2, kq.poll());

		Thread.sleep(100);

		Assert.assertEquals("not equal", payload3, kq.peek());
		Assert.assertEquals("not equal", payload3, kq.remove());

		Thread.sleep(100);

		Assert.assertNull("not null", kq.poll());
	}

	@Test(expected = NoSuchElementException.class)
	public void testKeptQueueEmptyElement() throws IOException, KeeperException, InterruptedException {
		KeptQueue kq = new KeptQueue(this.keeper, TestKeptQueue.PARENT, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		kq.element();
	}

	@Test(expected = NoSuchElementException.class)
	public void testKeptQueueEmptyRemove() throws IOException, KeeperException, InterruptedException {
		KeptQueue kq = new KeptQueue(this.keeper, TestKeptQueue.PARENT, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		kq.remove();
	}
}
