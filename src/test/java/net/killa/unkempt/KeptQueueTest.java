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
import java.util.NoSuchElementException;

import net.killa.kept.BaseKeptUtil;
import net.killa.kept.KeptQueue;
import net.killa.kept.SerializablePerson;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Test;

public class KeptQueueTest extends BaseKeptUtil {
    {
	this.parent = "/testunkemptqueue";
    }

    @Test
    public void testKeptStringQueue() throws Exception {
	final UnkemptQueue<String> kq = new UnkemptQueue<String>(String.class,
		this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	final String payload1 = Long.toString(System.currentTimeMillis());
	Thread.sleep(100);
	final String payload2 = Long.toString(System.currentTimeMillis());
	Thread.sleep(100);
	final String payload3 = Long.toString(System.currentTimeMillis());

	Assert.assertNull("not null", kq.peek());

	kq.offer(payload1);

	kq.offer(payload2);

	kq.offer(payload3);

	Assert.assertEquals("not equal", payload1, kq.peek());
	Assert.assertEquals("not equal", payload1, kq.remove());

	Assert.assertEquals("not equal", payload2, kq.element());
	Assert.assertEquals("not equal", payload2, kq.poll());

	Assert.assertEquals("not equal", payload3, kq.peek());
	Assert.assertEquals("not equal", payload3, kq.remove());

	Assert.assertNull("not null", kq.poll());
    }

    @Test
    public void testKeptNonPrimitiveQueue() throws Exception {
	final UnkemptQueue<SerializablePerson> kq = new UnkemptQueue<SerializablePerson>(
		SerializablePerson.class, this.keeper, this.parent,
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	final SerializablePerson person1 = new SerializablePerson();
	person1.setAge(50);
	person1.setName("person1");
	final SerializablePerson person2 = new SerializablePerson();
	person2.setAge(60);
	person2.setName("person2");
	final SerializablePerson person3 = new SerializablePerson();
	person3.setAge(70);
	person3.setName("person3");

	Assert.assertNull("not null", kq.peek());

	kq.offer(person1);

	kq.offer(person2);

	kq.offer(person3);

	Assert.assertEquals("not equal", person1, kq.peek());
	Assert.assertEquals("not equal", person1, kq.remove());

	Assert.assertEquals("not equal", person2, kq.element());
	Assert.assertEquals("not equal", person2, kq.poll());

	Assert.assertEquals("not equal", person3, kq.peek());
	Assert.assertEquals("not equal", person3, kq.remove());

	Assert.assertNull("not null", kq.poll());
    }

    @Test(expected = NoSuchElementException.class)
    public void testKeptQueueEmptyElement() throws IOException,
	    KeeperException, InterruptedException {
	final KeptQueue<Object> kq = new KeptQueue<Object>(Object.class,
		this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	kq.element();
    }

    @Test(expected = NoSuchElementException.class)
    public void testKeptQueueEmptyRemove() throws IOException, KeeperException,
	    InterruptedException {
	final KeptQueue<Object> kq = new KeptQueue<Object>(Object.class,
		this.keeper, this.parent, Ids.OPEN_ACL_UNSAFE,
		CreateMode.EPHEMERAL);

	kq.remove();
    }
}