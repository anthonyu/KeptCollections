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
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class KeptTestBase {
    private static final int CLIENT_PORT = new Random().nextInt(55535) + 10000;

    private static NIOServerCnxnFactory nioServerCnxnFactory;

    protected ZooKeeper keeper;

    @BeforeClass
    public static void init() throws IOException, InterruptedException {
	final File dir = new File(System.getProperty("java.io.tmpdir"),
		"zookeeper").getAbsoluteFile();
	final ZooKeeperServer server = new ZooKeeperServer(dir, dir, 2000);

	KeptTestBase.nioServerCnxnFactory = new NIOServerCnxnFactory();
	KeptTestBase.nioServerCnxnFactory.configure(new InetSocketAddress(
		KeptTestBase.CLIENT_PORT), 5000);

	KeptTestBase.nioServerCnxnFactory.startup(server);
    }

    @Before
    public void before() throws IOException, InterruptedException,
	    KeeperException {
	final CountDownLatch latch = new CountDownLatch(1);

	final CountDownOnConnectWatcher watcher = new CountDownOnConnectWatcher();
	watcher.setLatch(latch);
	this.keeper = new ZooKeeper("localhost:"
		+ Integer.toString(KeptTestBase.CLIENT_PORT), 20000, watcher);
	if (!latch.await(5, TimeUnit.SECONDS))
	    throw new RuntimeException("unable to connect to server");
    }

    @After
    public void after() throws InterruptedException, KeeperException {
	final String parent = this.getParent();

	for (final String s : this.keeper.getChildren(parent, false))
	    this.keeper.delete(parent + '/' + s, -1);

	this.keeper.close();
    }

    @AfterClass
    public static void afterClass() {
	KeptTestBase.nioServerCnxnFactory.shutdown();
    }

    public abstract String getParent();
}
