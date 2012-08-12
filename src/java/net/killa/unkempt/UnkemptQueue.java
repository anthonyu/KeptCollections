package net.killa.unkempt;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

public class UnkemptQueue<T> extends UnkemptCollection<T> implements Queue<T> {
    public UnkemptQueue(final Class<? extends T> elementClass,
	    final ZooKeeper keeper, final String znode, final List<ACL> acl,
	    final CreateMode createMode) throws KeeperException,
	    InterruptedException {
	super(elementClass, keeper, znode, acl, createMode);
    }

    @Override
    public T element() {
	final T e = this.peek();

	if (e == null)
	    throw new NoSuchElementException();

	return e;
    }

    @Override
    public boolean offer(final T e) {
	// no capacity limitations for this queue (yet)
	return this.add(e);
    }

    @Override
    public T peek() {
	try {
	    return this.getFirst(false);
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
    public T poll() {
	try {
	    return this.getFirst(true);
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
    public T remove() {
	final T e = this.poll();

	if (e == null)
	    throw new NoSuchElementException();

	return e;
    }
}
