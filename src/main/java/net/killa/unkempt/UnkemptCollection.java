package net.killa.unkempt;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import net.killa.kept.Transformer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * un·kempt/ˌənˈkem(p)t/ Adjective: (esp. of a person) Having an untidy or
 * disheveled appearance.
 * 
 * @author anthonyu
 * 
 * @param <T>
 *            Any serializeable type.
 */
public class UnkemptCollection<T> implements Collection<T> {
    private final ArrayList<String> cache;
    private final Class<? extends T> elementClass;
    private final ZooKeeper keeper;
    private final String znode;
    private final List<ACL> acl;
    private final CreateMode createMode;

    public UnkemptCollection(final Class<? extends T> elementClass,
	    final ZooKeeper keeper, final String znode, final List<ACL> acl,
	    final CreateMode createMode) throws KeeperException,
	    InterruptedException {
	this.cache = new ArrayList<String>();
	this.elementClass = elementClass;
	this.keeper = keeper;

	// if the znode doesn't exist, create a permanent znode with that path
	// TODO: change to allow ephemeral znode when ephemeral parents are
	// supported by zookeeper
	try {
	    if (this.keeper.exists(znode, false) == null)
		this.keeper.create(znode, new byte[0], acl,
			CreateMode.PERSISTENT);
	} catch (final KeeperException.NodeExistsException e) {
	    // ignore this exception
	}

	this.znode = znode;
	this.acl = acl;
	if (createMode == CreateMode.PERSISTENT_SEQUENTIAL
		|| createMode == CreateMode.EPHEMERAL_SEQUENTIAL)
	    this.createMode = createMode;
	else if (createMode == CreateMode.PERSISTENT)
	    this.createMode = CreateMode.PERSISTENT_SEQUENTIAL;
	else if (createMode == CreateMode.EPHEMERAL)
	    this.createMode = CreateMode.EPHEMERAL_SEQUENTIAL;
	else
	    throw new InvalidParameterException("unexpected create mode "
		    + createMode.toString());

	this.fillCache();
    }

    private void fillCache() throws KeeperException, InterruptedException {
	if (this.cache.isEmpty())
	    for (final String child : this.keeper
		    .getChildren(this.znode, false))
		this.cache.add(this.znode + '/' + child);

	Collections.sort(this.cache);
    }

    public List<T> getFirstN(final int max) throws KeeperException,
	    InterruptedException, IOException, ClassNotFoundException {
	final List<T> es = new ArrayList<T>();

	for (int i = 0; i < max; i++) {
	    final T e = this.getFirst(true);

	    if (e == null)
		return es;

	    es.add(e);
	}

	return es;
    }

    @SuppressWarnings("unchecked")
    public T getFirst(final boolean delete) throws KeeperException,
	    InterruptedException, IOException, ClassNotFoundException {
	for (final Iterator<String> it = this.cache.iterator(); it.hasNext();) {
	    final String child = it.next();

	    final Stat stat = this.keeper.exists(child, false);

	    if (stat == null) {
		it.remove();

		continue;
	    }

	    final T e = (T) Transformer.bytesToObject(
		    this.keeper.getData(child, false, stat), this.elementClass);

	    if (delete) {
		this.keeper.delete(child, stat.getVersion());

		it.remove();
	    }

	    return e;
	}

	this.fillCache();

	if (!this.cache.isEmpty())
	    return this.getFirst(delete);

	return null;
    }

    @Override
    public boolean addAll(final Collection<? extends T> c) {
	for (final T e : c)
	    if (!this.add(e))
		return false;

	return true;
    }

    @Override
    public void clear() {
	synchronized (this.cache) {
	    try {
		for (final String child : this.keeper.getChildren(this.znode,
			false))
		    this.keeper.delete(this.znode + '/' + child, -1);

		this.cache.clear();
	    } catch (final KeeperException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    } catch (final InterruptedException e) {
		throw new RuntimeException(e.getClass().getSimpleName()
			+ " caught", e);
	    }
	}
    }

    @Override
    public boolean contains(final Object o) {
	throw new UnsupportedOperationException("contains is not supported");

    }

    @Override
    public boolean containsAll(final Collection<?> c) {
	throw new UnsupportedOperationException("containsAll is not supported");
    }

    @Override
    public boolean isEmpty() {
	return this.size() == 0;
    }

    @Override
    public Iterator<T> iterator() {
	try {
	    this.cache.clear();
	    this.fillCache();
	    return new Iterator<T>() {
		Iterator<String> it = UnkemptCollection.this.cache.iterator();

		@Override
		public boolean hasNext() {
		    return this.it.hasNext();
		}

		@SuppressWarnings("unchecked")
		@Override
		public T next() {
		    while (this.it.hasNext()) {
			final String path = this.it.next();
			try {
			    final Stat stat = UnkemptCollection.this.keeper
				    .exists(path, false);
			    if (stat == null)
				continue;

			    return (T) Transformer.bytesToObject(
				    UnkemptCollection.this.keeper.getData(path,
					    false, stat),
				    UnkemptCollection.this.elementClass);
			} catch (final KeeperException e) {
			    throw new RuntimeException(e.getClass()
				    .getSimpleName() + " caught", e);
			} catch (final InterruptedException e) {
			    throw new RuntimeException(e.getClass()
				    .getSimpleName() + " caught", e);
			} catch (final IOException e) {
			    throw new RuntimeException(e.getClass()
				    .getSimpleName() + " caught", e);
			} catch (final ClassNotFoundException e) {
			    throw new RuntimeException(e.getClass()
				    .getSimpleName() + " caught", e);
			}
		    }

		    return null;
		}

		@Override
		public void remove() {
		    throw new UnsupportedOperationException(
			    "remove is not supported");
		}
	    };
	} catch (final KeeperException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	} catch (final InterruptedException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	}
    }

    @Override
    public boolean remove(final Object o) {
	throw new UnsupportedOperationException("remove is not supported");
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
	for (final Object o : c)
	    if (!this.remove(o))
		return false;

	return true;
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
	throw new UnsupportedOperationException("retainAll is not supported");
    }

    @Override
    public int size() {
	try {
	    return this.keeper.exists(this.znode, false).getNumChildren();
	} catch (final KeeperException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	} catch (final InterruptedException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	}
    }

    @Override
    public Object[] toArray() {
	throw new UnsupportedOperationException("toArray is not supported");
    }

    @SuppressWarnings("hiding")
    @Override
    public <T> T[] toArray(final T[] a) {
	throw new UnsupportedOperationException("toArray is not supported");
    }

    @Override
    public boolean add(final T o) {
	try {
	    this.keeper.create(this.znode + "/entry-",
		    Transformer.objectToBytes(o, this.elementClass), this.acl,
		    this.createMode);

	    return true;
	} catch (final KeeperException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	} catch (final InterruptedException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	} catch (final IOException e) {
	    throw new RuntimeException(
		    e.getClass().getSimpleName() + " caught", e);
	}
    }
}
