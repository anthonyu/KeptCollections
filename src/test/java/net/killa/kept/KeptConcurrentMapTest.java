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

public class KeptConcurrentMapTest extends KeptTestBase {
    @Test
    public void testKeptConcurrentMap() throws IOException, KeeperException,
	    InterruptedException {
	final KeptConcurrentMap kcm = new KeptConcurrentMap(String.class, this.keeper,
		this.getParent(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	final String payload1 = Long.toString(System.currentTimeMillis());

	Assert.assertNull("not null", kcm.putIfAbsent("test", payload1));

	Thread.sleep(100);

	final String payload2 = Long.toString(System.currentTimeMillis());

	Assert.assertEquals("not equal", payload1,
		kcm.putIfAbsent("test", payload2));

	Thread.sleep(100);

	Assert.assertEquals("not equal", payload1,
		kcm.putIfAbsent("test", payload2));

	Thread.sleep(100);

	Assert.assertFalse("not false", kcm.remove("test", payload2));

	Thread.sleep(100);

	Assert.assertTrue("not true", kcm.remove("test", payload1));

	Thread.sleep(100);

	Assert.assertFalse("not false", kcm.remove("test", payload1));

	Thread.sleep(100);

	Assert.assertNull("not null", kcm.replace("test", payload1));

	Thread.sleep(100);

	Assert.assertNull("not null", kcm.replace("test", payload1));

	Thread.sleep(100);

	kcm.put("test", payload1);

	Thread.sleep(100);

	Assert.assertEquals("not equal", payload1,
		kcm.replace("test", payload2));

	Thread.sleep(100);

	Assert.assertEquals("not equal", payload2, kcm.get("test"));

	Assert.assertTrue("not true", kcm.replace("test", payload2, payload1));

	Thread.sleep(100);

	Assert.assertEquals("not equal", payload1, kcm.get("test"));

	Assert.assertFalse("not false", kcm.replace("test",
		Long.toString(System.currentTimeMillis()), payload2));

	Thread.sleep(100);

	Assert.assertEquals("not equal", payload1, kcm.get("test"));

	Assert.assertFalse("not false", kcm.replace("uest",
		Long.toString(System.currentTimeMillis()), payload2));
    }

    @Override
    public String getParent() {
	return "/testconcurrentmap";
    }
}
