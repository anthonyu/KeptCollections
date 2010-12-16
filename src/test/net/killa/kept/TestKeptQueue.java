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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Test;

public class TestKeptQueue extends BaseKeptUtil {
    {
	this.parent = "/testkeptqueue";
    }

    @Test
    public void testKeptStringQueue() throws Exception {
	KeptQueue<String> kq = new KeptQueue<String>(this.keeper, this.parent,
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

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

    @Test
    public void testKeptNonPrimitiveQueue() throws Exception {
	KeptQueue<SerializablePerson> kq = new KeptQueue<SerializablePerson>(
		this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	SerializablePerson person1 = new SerializablePerson();
	person1.age = 50;
	person1.name = "person1";
	SerializablePerson person2 = new SerializablePerson();
	person2.age = 60;
	person2.name = "person2";
	SerializablePerson person3 = new SerializablePerson();
	person3.age = 70;
	person3.name = "person3";

	Assert.assertNull("not null", kq.peek());

	kq.offer(person1);
	Thread.sleep(100);

	kq.offer(person2);
	Thread.sleep(100);

	kq.offer(person3);
	Thread.sleep(100);

	Assert.assertEquals("not equal", person1, kq.peek());
	Assert.assertEquals("not equal", person1, kq.remove());
	Thread.sleep(100);

	Assert.assertEquals("not equal", person2, kq.element());
	Assert.assertEquals("not equal", person2, kq.poll());
	Thread.sleep(100);

	Assert.assertEquals("not equal", person3, kq.peek());
	Assert.assertEquals("not equal", person3, kq.remove());
	Thread.sleep(100);

	Assert.assertNull("not null", kq.poll());
    }

    @Test(expected = NoSuchElementException.class)
    public void testKeptQueueEmptyElement() throws IOException,
	    KeeperException, InterruptedException {
	KeptQueue<Object> kq = new KeptQueue<Object>(this.keeper, this.parent,
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	kq.element();
    }

    @Test(expected = NoSuchElementException.class)
    public void testKeptQueueEmptyRemove() throws IOException, KeeperException,
	    InterruptedException {
	KeptQueue<Object> kq = new KeptQueue<Object>(this.keeper, this.parent,
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	kq.remove();
    }
}
