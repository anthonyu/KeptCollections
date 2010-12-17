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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Test;

public class TestKeptList extends BaseKeptUtil {
    {
	this.parent = "/testkeptlist";
    }

    @Test
    public void testKeptStringList() throws Exception {
	KeptList<String> kl = new KeptList<String>(String.class, this.keeper,
		this.parent, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	String payload = Long.toString(System.currentTimeMillis());
	Thread.sleep(100);
	kl.add(Long.toString(System.currentTimeMillis()));
	Thread.sleep(100);
	kl.add(payload);
	Thread.sleep(100);
	kl.add(Long.toString(System.currentTimeMillis()));
	Thread.sleep(100);
	kl.add(payload);
	Thread.sleep(100);

	Assert.assertEquals("wrong index", 1, kl.indexOf(payload));
	Assert.assertEquals("wrong index", 3, kl.lastIndexOf(payload));
	Assert.assertEquals("not equal", payload, kl.get(1));
	Assert.assertEquals("not equal", payload, kl.remove(3));
	Thread.sleep(100);
	Assert.assertEquals("wrong index", 1, kl.indexOf(payload));
	Assert.assertEquals("wrong index", 1, kl.lastIndexOf(payload));

	kl.set(2, payload);
	Thread.sleep(1000);
	Assert.assertEquals("wrong index", 1, kl.indexOf(payload));
	Assert.assertEquals("wrong index", 2, kl.lastIndexOf(payload));
    }

    @Test
    public void testKeptLongList() throws Exception {
	KeptList<Long> kl = new KeptList<Long>(Long.class, this.keeper,
		this.parent, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	long payload = System.currentTimeMillis();
	Thread.sleep(100);
	kl.add(System.currentTimeMillis());
	Thread.sleep(100);
	kl.add(payload);
	Thread.sleep(100);
	kl.add(System.currentTimeMillis());
	Thread.sleep(100);
	kl.add(payload);
	Thread.sleep(100);

	Assert.assertEquals("wrong index", 1, kl.indexOf(payload));
	Assert.assertEquals("wrong index", 3, kl.lastIndexOf(payload));
	Assert.assertEquals(0D, payload, kl.get(1));
	Assert.assertEquals(0D, payload, kl.remove(3));
	Thread.sleep(100);
	Assert.assertEquals("wrong index", 1, kl.indexOf(payload));
	Assert.assertEquals("wrong index", 1, kl.lastIndexOf(payload));

	kl.set(2, payload);
	Thread.sleep(1000);
	Assert.assertEquals("wrong index", 1, kl.indexOf(payload));
	Assert.assertEquals("wrong index", 2, kl.lastIndexOf(payload));
    }

    @Test
    public void testKeptNonprimitiveList() throws Exception {
	KeptList<SerializablePerson> kl = new KeptList<SerializablePerson>(
		SerializablePerson.class, this.keeper, this.parent,
		Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	SerializablePerson person1 = new SerializablePerson();
	person1.age = 100;
	person1.name = "person1";
	SerializablePerson person2 = new SerializablePerson();
	person2.age = 90;
	person2.name = "person2";

	kl.add(person1);
	Thread.sleep(100);
	kl.add(person2);
	Thread.sleep(100);
	kl.add(person1);
	Thread.sleep(100);
	kl.add(person2);
	Thread.sleep(100);

	Assert.assertEquals("wrong index", 0, kl.indexOf(person1));
	Assert.assertEquals("wrong index", 2, kl.lastIndexOf(person1));
	Assert.assertEquals("wrong index", 1, kl.indexOf(person2));
	Assert.assertEquals("wrong index", 3, kl.lastIndexOf(person2));
	Assert.assertEquals(person1, kl.get(0));
	Assert.assertEquals(person2, kl.get(1));
	Assert.assertEquals(person1, kl.get(2));
	Assert.assertEquals(person2, kl.get(3));

	Assert.assertEquals(person1, kl.remove(2));
	Thread.sleep(100);
	Assert.assertEquals("wrong index", 0, kl.indexOf(person1));
	Assert.assertEquals("wrong index", 0, kl.lastIndexOf(person1));

	kl.set(2, person1);
	Thread.sleep(1000);
	Assert.assertEquals("wrong index", 0, kl.indexOf(person1));
	Assert.assertEquals("wrong index", 2, kl.lastIndexOf(person1));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testKeptCollectionBigIndex() throws KeeperException,
	    InterruptedException {
	KeptList<String> kl = new KeptList<String>(String.class, this.keeper,
		this.parent, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	kl.set(Integer.MAX_VALUE, "wtf");
    }

}
