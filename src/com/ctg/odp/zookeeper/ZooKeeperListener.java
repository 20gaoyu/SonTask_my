package com.ctg.odp.zookeeper;

/**
 * Base class for internal listeners of ZooKeeper events.
 *
 * The {@link ZooKeeperWatcher} for a process will execute the appropriate
 * methods of implementations of this class. In order to receive events from the
 * watcher, every listener must register itself via
 * {@link ZooKeeperWatcher#registerListener}.
 *
 * Subclasses need only override those methods in which they are interested.
 *
 * Note that the watcher will be blocked when invoking methods in listeners so
 * they must not be long-running.
 */
public abstract class ZooKeeperListener {

    // Reference to the zk watcher which also contains configuration and
    // constants
    protected ZooKeeperWatcher watcher;

    /**
     * Construct a ZooKeeper event listener.
     */
    public ZooKeeperListener(ZooKeeperWatcher watcher) {
        this.watcher = watcher;
    }

    /**
     * Called when a new node has been created.
     * 
     * @param path
     *            full path of the new node
     */
    public void nodeCreated(String path) {
        // no-op
    }

    /**
     * Called when a node has been deleted
     * 
     * @param path
     *            full path of the deleted node
     */
    public void nodeDeleted(String path) {
        // no-op
    }

    /**
     * Called when an existing node has changed data.
     * 
     * @param path
     *            full path of the updated node
     */
    public void nodeDataChanged(String path) {
        // no-op
    }

    /**
     * Called when an existing node has a child node added or removed.
     * 
     * @param path
     *            full path of the node whose children have changed
     */
    public void nodeChildrenChanged(String path) {
        // no-op
    }
}
