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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class BaseKeptUtil {
    private static final int CLIENT_PORT = new Random().nextInt(55535) + 10000;

    private static NIOServerCnxn.Factory nioServerCnxnFactory;

    protected String parent;
    protected ZooKeeper keeper;

    @BeforeClass
    public static void init() throws IOException, InterruptedException {
	final File dir = new File(System.getProperty("java.io.tmpdir"),
		"zookeeper").getAbsoluteFile();
	final ZooKeeperServer server = new ZooKeeperServer(dir, dir, 2000);

	BaseKeptUtil.nioServerCnxnFactory = new NIOServerCnxn.Factory(
		new InetSocketAddress(BaseKeptUtil.CLIENT_PORT), 5000);

	BaseKeptUtil.nioServerCnxnFactory.startup(server);
    }

    @Before
    public void before() throws IOException, InterruptedException,
	    KeeperException {
	CountDownLatch latch = new CountDownLatch(1);

	CountDownOnConnectWatcher watcher = new CountDownOnConnectWatcher();
	watcher.setLatch(latch);
	this.keeper = new ZooKeeper("localhost:"
		+ Integer.toString(BaseKeptUtil.CLIENT_PORT), 20000, watcher);
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

    @AfterClass
    public static void afterClass() {
	BaseKeptUtil.nioServerCnxnFactory.shutdown();
    }
}
