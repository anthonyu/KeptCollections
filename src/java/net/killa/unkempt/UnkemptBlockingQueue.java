package net.killa.unkempt;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

public class UnkemptBlockingQueue<T> extends UnkemptQueue<T> implements
	BlockingQueue<T> {
    public UnkemptBlockingQueue(final Class<? extends T> elementClass,
	    final ZooKeeper keeper, final String znode, final List<ACL> acl,
	    final CreateMode createMode) throws KeeperException,
	    InterruptedException {
	super(elementClass, keeper, znode, acl, createMode);
    }

    @Override
    public int drainTo(final Collection<? super T> c) {
	return this.drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(final Collection<? super T> c, final int maxElements) {
	try {
	    final List<T> es = this.getFirstN(maxElements);

	    c.addAll(es);

	    return es.size();
	} catch (final KeeperException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	} catch (final InterruptedException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	} catch (final IOException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	} catch (final ClassNotFoundException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	}
    }

    @Override
    public boolean offer(final T o, final long timeout, final TimeUnit unit)
	    throws InterruptedException {
	// no capacity limitations for this queue (yet)
	return this.offer(o);
    }

    @Override
    public T poll(final long timeout, final TimeUnit unit)
	    throws InterruptedException {
	final long last = System.currentTimeMillis()
		+ TimeUnit.MILLISECONDS.convert(timeout, unit);

	do {
	    final T element = this.poll();

	    if (element != null)
		return element;

	    Thread.sleep(100);
	} while (System.currentTimeMillis() <= last);

	return null;
    }

    @Override
    public void put(final T e) throws InterruptedException {
	this.offer(e);
    }

    @Override
    public int remainingCapacity() {
	return Integer.MAX_VALUE;
    }

    @Override
    public T take() throws InterruptedException {
	return this.poll(Long.MAX_VALUE, TimeUnit.DAYS);
    }
}
