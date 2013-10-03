package net.killa.kept;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Test;

public class KeptSortedSetTest extends KeptTestBase{
	
	@Test
    public void testKeptTreeSet() throws IOException, KeeperException,
	    InterruptedException {
		final String parent = this.getParent();

		final KeptSortedSet ks = new KeptSortedSet(this.keeper, parent,
			Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		
		// check to see that changes made to the set are reflected in the znode
		String znode = Long.toString(System.currentTimeMillis());

		Assert.assertFalse(ks.contains(znode));
		
		ks.add(znode);

		Assert.assertNotNull(this.keeper.exists(parent + '/' + znode, null));
		
		this.keeper.delete(parent + '/' + znode, -1);

		// wait for it to take effect
		Thread.sleep(50);
		Assert.assertFalse(ks.contains(znode));
		
		// check to see that changes on zookeeper are reflected in the set
		znode = Long.toString(System.currentTimeMillis());
		Assert.assertFalse(ks.contains(znode));
		
		this.keeper.create(parent + '/' + znode, new byte[0],
				Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		// wait for it to take effect
		Thread.sleep(50);
		Assert.assertTrue(ks.contains(znode));
		
		ks.remove(znode);

		Assert.assertNull(this.keeper.exists(parent + '/' + znode, null));
		
	}
	
	@Test
    public void testKeptTreeSetReAdd() throws IOException, KeeperException,
	    InterruptedException {
		final KeptSortedSet ks = new KeptSortedSet(this.keeper, this.getParent(),
			Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	
		// check to see that changes made to the set are reflected in the znode
		final String znode = Long.toString(System.currentTimeMillis());
	
		Assert.assertFalse(ks.contains(znode));
	
		Assert.assertTrue(ks.add(znode));
		Assert.assertFalse(ks.add(znode));
	
		Thread.sleep(50);
	
		Assert.assertFalse(ks.add(znode));
    }
	
	@Test
    public void testKeptTreeSetClear() throws IOException, KeeperException,
	    InterruptedException {
		final KeptSortedSet ks = new KeptSortedSet(this.keeper, this.getParent(),
			Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	
		ks.add("one");
		ks.add("two");
		ks.add("three");
	
		// wait for it to take effect
		Thread.sleep(50);
		
		Assert.assertTrue("set does not contain one", ks.contains("one"));
		Assert.assertTrue("set does not contain two", ks.contains("two"));
		Assert.assertTrue("set does not contain three", ks.contains("three"));
	
		ks.clear();
	
		// wait for it to take effect
		Thread.sleep(50);
	
		Assert.assertTrue("set is not empty", ks.isEmpty());
	
		Assert.assertEquals("set is not empty", 0, ks.size());
    }
	
	@Test
    public void testKeptTreeSetAddAll() throws IOException, KeeperException,
	    InterruptedException {
		final Set<String> hs = new HashSet<String>();
	
		hs.add("one");
		hs.add("two");
		hs.add("three");
	
		final KeptSortedSet s = new KeptSortedSet(this.keeper, this.getParent(),
			Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	
		s.addAll(hs);
	
		// wait for it to take effect
		Thread.sleep(50);
	
		Assert.assertTrue("set does not contain all", s.containsAll(hs));
    }

    @Test
    public void testKeptTreeSetRetainAll() throws IOException, KeeperException,
	    InterruptedException {
		final Set<String> hs1 = new HashSet<String>();
	
		hs1.add("one");
		hs1.add("two");
		hs1.add("three");
	
		final Set<String> hs2 = new HashSet<String>();
	
		hs2.add("two");
		hs2.add("three");
		hs2.add("four");
	
		final KeptSortedSet ks = new KeptSortedSet(this.keeper, this.getParent(),
			Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	
		ks.addAll(hs1);
	
		// wait for it to take effect
		Thread.sleep(50);
	
		ks.retainAll(hs2);
		hs1.retainAll(hs2);
	
		// wait for it to take effect
		Thread.sleep(50);
	
		Assert.assertTrue("set does not contain all", ks.containsAll(hs1));
		Assert.assertEquals("sets are not the same size", hs1.size(), ks.size());
		/*System.out.print("###############################################################################");
		for(String s : ks)
			System.out.print(s + " ");*/
    }
    
    @Test
    public void testKeptTreeSetSort() throws IOException, KeeperException,
	    InterruptedException {
		final Set<String> hs1 = new TreeSet<String>();
	
		hs1.add("apple");
		hs1.add("boy");
		hs1.add("cat");
		hs1.add("dog");
	
		final KeptSortedSet ks = new KeptSortedSet(this.keeper, this.getParent(),
			Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	
		ks.addAll(hs1);
	
		// wait for it to take effect
		Thread.sleep(50);
		
		java.util.Iterator<String> iSet = hs1.iterator();
		java.util.Iterator<String> iks = ks.iterator();
		while(iSet.hasNext()){
			Assert.assertEquals("sets are not the same", iSet.next(), iks.next());
		}
	
		//Assert.assertTrue("set does not contain all", ks.containsAll(hs1));
		//Assert.assertEquals("sets are not the same size", hs1.size(), ks.size());
		/*System.out.print("###############################################################################");
		for(String s : ks)
			System.out.print(s + " ");*/
    }

	@Override
    public String getParent() {
		return "/testkeptset";
    }

}
