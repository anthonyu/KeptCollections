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

import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Test;

public class TestKeptLock extends BaseKeptUtil {
    private static final String LOCK = "/testkeptlock";

    @Override
    @After
    public void after() throws KeeperException, InterruptedException {
	try {
	    this.keeper.delete(TestKeptLock.LOCK, -1);
	} catch (KeeperException.NoNodeException e) {
	    // ignore it
	}
	this.keeper.close();
    }

    @Test
    public void testKeptLock() throws KeeperException, InterruptedException {
	KeptLock kl = new KeptLock(this.keeper, TestKeptLock.LOCK,
		Ids.OPEN_ACL_UNSAFE);

	Assert.assertTrue("cannot lock", kl.tryLock());
	Assert.assertFalse("can lock", kl.tryLock());

	kl.unlock();
	kl.lock();

	Assert.assertFalse("can lock", kl.tryLock());

	kl.unlock();
	kl.lockInterruptibly();

	Assert.assertFalse("can lock", kl.tryLock());
    }

    @Test
    public void testKeptLockWait() throws KeeperException, InterruptedException {
	KeptLock kl = new KeptLock(this.keeper, TestKeptLock.LOCK,
		Ids.OPEN_ACL_UNSAFE);

	long now = System.currentTimeMillis();
	Assert.assertTrue("cannot lock", kl.tryLock(5, TimeUnit.SECONDS));

	Assert.assertTrue("time is way out of proportion",
		System.currentTimeMillis() - now < 100);

	Assert.assertFalse("can lock", kl.tryLock(5, TimeUnit.SECONDS));

	Assert.assertTrue("time is way out of proportion",
		System.currentTimeMillis() - now < 6000);
    }

    @Test(expected = IllegalStateException.class)
    public void testKeptLockUnlockUnlocked() throws KeeperException,
	    InterruptedException {
	KeptLock kl = new KeptLock(this.keeper, TestKeptLock.LOCK,
		Ids.OPEN_ACL_UNSAFE);

	kl.unlock();
    }
}
