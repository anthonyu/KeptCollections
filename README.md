Welcome to the KeptCollections FAQ
==================================

What is KeptCollections?
------------------------

KeptCollections is a library of drop-in replacements for the data
structures in the Java Collections framework. KeptCollections uses Apache
ZooKeeper as a backing store, thus making its data structures distributed
and scalable.

Changes made to a KeptCollection by one node are seen by all other nodes
within milliseconds, allowing for easy communication between nodes in a
computing cluster.

KeptCollections now supports Java generics, thanks to Gaurav Sharma. Classes
that implement Serializable can be used as values in any of the
collections. Classes that are serializable as strings (i.e. ints, doubles,
etc.) will still be stored as strings on the ZooKeeper.

Why KeptCollections?
--------------------

Implementing a distributed data structure from scratch is difficult.
ZooKeeper programming is less hard, but still no walk in the park.
Conversely, KeptCollections implements the well known Java Collections
APIs, so they can be dropped into new or existing Java code and
easily make those applications distributed.

How does one use KeptCollections?
---------------------------------

The only difference between the KeptCollections collections and the JDK 
collections are the names of the classes and their constructors.

For instance, where a Map from Java Collections could be instantiated like:

<code>
    Map<String, String> map = new HashMap<String, String>();
</code>

KeptCollections is instead instantiated like:

<code>
    Zookeeper zk = new ZooKeeper("localhost:2181", 20000, watcher);

    Map<String, String> map =
      new KeptMap(zk, "/mymap", Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
</code>

Both are accessed the same way:

<code>
    map.put("key", "value");

    String value = map.get("key");
</code>

See the JavaDoc help for more information.

How stable are the collections?
-------------------------------

The KeptSet, KeptMap and KeptLock implementations have seen several years
of usage in a production environment at two different cloud computing
companies.

The other implementations are similar in code base and well unit tested,
but have not seen production usage.

Which Collections interfaces are implemented?
---------------------------------------------

Set


Map

ConcurrentMap


Collection

List

Queue

BlockingQueue


Lock (not from Java Collections, but it sort of fits here regardless).

Which interfaces will (likely) be implemented in the future?
------------------------------------------------------------

SortedSet


SortedMap

A few of the java.concurrent interfaces, but not those, mainly
lock and barrier related, that are already implemented by Menagerie
[https://github.com/openUtility/menagerie]

Where can I get a Jar?
----------------------

Right <a href="https://github.com/downloads/anthonyu/KeptCollections/kept-collections-0.9.1.jar">here</a>.
