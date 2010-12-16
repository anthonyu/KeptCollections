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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;

public class BaseKeptUtil {
    protected String parent;
    protected ZooKeeper keeper;

    @Before
    public void before() throws IOException, InterruptedException,
	    KeeperException {
	CountDownLatch latch = new CountDownLatch(1);

	// FIXME: set up a zookeeper server in process
	CountDownOnConnectWatcher watcher = new CountDownOnConnectWatcher();
	watcher.setLatch(latch);
	this.keeper = new ZooKeeper("localhost:2181", 20000, watcher);
	if (!latch.await(5, TimeUnit.SECONDS)) {
	    throw new RuntimeException("unable to connect to server");
	}
    }

    @After
    public void after() throws InterruptedException, KeeperException {
	for (String s : this.keeper.getChildren(this.parent, false)) {
	    this.keeper.delete(this.parent + '/' + s, -1);
	}
	this.keeper.close();
    }
}
