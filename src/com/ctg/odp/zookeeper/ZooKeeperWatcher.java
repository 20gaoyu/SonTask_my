package com.ctg.odp.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

/**
 * Acts as the single ZooKeeper Watcher. One instance of this is instantiated for each Master, RegionServer, and client process.
 * 
 * <p>
 * This is the only class that implements {@link Watcher}. Other internal classes which need to be notified of ZooKeeper events must register with the local instance of this watcher via {@link #registerListener}.
 * 
 * <p>
 * This class also holds and manages the connection to ZooKeeper. Code to deal with connection related events and exceptions are handled here.
 */
public class ZooKeeperWatcher implements Watcher, Abortable {
    private static final Log LOG = LogFactory.getLog(ZooKeeperWatcher.class);

    // Identifier for this watcher (for logging only). It is made of the prefix
    // passed on construction and the zookeeper sessionid.
    private String identifier;

    // zookeeper quorum
    private final String quorum;

    // zookeeper connection
    private final RecoverableZooKeeper recoverableZooKeeper;

    // abortable in case of zk failure
    private final Abortable abortable;

    // listeners to be notified
    private final List<ZooKeeperListener> listeners = new CopyOnWriteArrayList<ZooKeeperListener>();

    // node names
    private String rootZNode;

    // Certain ZooKeeper nodes need to be world-readable
    public static final List<ACL> CREATOR_ALL_AND_WORLD_READABLE = new ArrayList<ACL>();
    static {
        CREATOR_ALL_AND_WORLD_READABLE.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
        CREATOR_ALL_AND_WORLD_READABLE.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS));
    }

    private final Configuration conf;

    private final Exception constructorCaller;

    /**
     * Instantiate a ZooKeeper connection and watcher.
     * 
     * @param descriptor
     *            Descriptive string that is added to zookeeper sessionid and used as identifier for this instance.
     * @throws IOException
     * @throws ZooKeeperConnectionException
     */
    public ZooKeeperWatcher(Configuration conf, String descriptor, Abortable abortable) throws IOException {
        this(conf, descriptor, abortable, false);
    }

    /**
     * Instantiate a ZooKeeper connection and watcher.
     * 
     * @param descriptor
     *            Descriptive string that is added to zookeeper sessionid and used as identifier for this instance.
     * @throws IOException
     * @throws ZooKeeperConnectionException
     */
    public ZooKeeperWatcher(Configuration conf, String descriptor, Abortable abortable, boolean canCreateBaseZNode) throws IOException {
        this.conf = conf;
        // Capture a stack trace now. Will print it out later if problem so we
        // can
        // distingush amongst the myriad ZKWs.
        this.constructorCaller = new Exception("ZKW CONSTRUCTOR STACK TRACE FOR DEBUGGING");
        this.quorum = ZKUtil.getQuorumServersString(conf);
        // Identifier will get the sessionid appended later below down when we
        // handle the syncconnect event.
        this.identifier = descriptor;
        this.abortable = abortable;
        this.recoverableZooKeeper = ZKUtil.connect(conf, quorum, this, descriptor);
        setNodeNames(conf);
        if (canCreateBaseZNode) {
            createBaseZNodes();
        }
    }

    private void createBaseZNodes() throws ZooKeeperConnectionException {
        try {
            // Create all the necessary "directories" of znodes
            ZKUtil.createAndFailSilent(this, getRootZNode());
        } catch (KeeperException e) {
            throw new ZooKeeperConnectionException(prefix("Unexpected KeeperException creating base node"), e);
        }
    }

    private void setNodeNames(Configuration conf) {
        setRootZNode(ZKUtil.getZNodeRoot(conf));

        LOG.info("rootZNode=" + getRootZNode());
    }

    @Override
    public String toString() {
        return this.identifier;
    }

    /**
     * Adds this instance's identifier as a prefix to the passed <code>str</code>
     * 
     * @param str
     *            String to amend.
     * @return A new string with this instance's identifier as prefix: e.g. if passed 'hello world', the returned string could be
     */
    public String prefix(final String str) {
        return this.toString() + " " + str;
    }

    /**
     * Register the specified listener to receive ZooKeeper events.
     * 
     * @param listener
     */
    public void registerListener(ZooKeeperListener listener) {
        listeners.add(listener);
    }

    /**
     * Register the specified listener to receive ZooKeeper events and add it as the first in the list of current listeners.
     * 
     * @param listener
     */
    public void registerListenerFirst(ZooKeeperListener listener) {
        listeners.add(0, listener);
    }

    /**
     * Get the connection to ZooKeeper.
     * 
     * @return connection reference to zookeeper
     */
    public RecoverableZooKeeper getRecoverableZooKeeper() {
        return recoverableZooKeeper;
    }

    /**
     * Get the quorum address of this instance.
     * 
     * @return quorum string of this zookeeper connection instance
     */
    public String getQuorum() {
        return quorum;
    }

    /**
     * Method called from ZooKeeper for events and connection status.
     * <p>
     * Valid events are passed along to listeners. Connection status changes are dealt with locally.
     */
    @Override
    public void process(WatchedEvent event) {
        LOG.info(prefix("Received ZooKeeper Event, " + "type=" + event.getType() + ", " + "state=" + event.getState() + ", " + "path=" + event.getPath()));

        switch (event.getType()) {
        case None: // If event type is NONE, this is a connection status change
            connectionEvent(event);
            break;
        case NodeCreated: // Otherwise pass along to the listeners
            for (ZooKeeperListener listener : listeners) {
                listener.nodeCreated(event.getPath());
            }
            break;
        case NodeDeleted:
            for (ZooKeeperListener listener : listeners) {
                listener.nodeDeleted(event.getPath());
            }
            break;
        case NodeDataChanged:
            for (ZooKeeperListener listener : listeners) {
                listener.nodeDataChanged(event.getPath());
            }
            break;
        case NodeChildrenChanged:
            for (ZooKeeperListener listener : listeners) {
                listener.nodeChildrenChanged(event.getPath());
            }
            break;
        default:
            break;
        }
    }

    // Connection management

    /**
     * Called when there is a connection-related event via the Watcher callback.
     * <p>
     * If Disconnected or Expired, this should shutdown the cluster. But, since we send a KeeperException.SessionExpiredException along with the abort call, it's possible for the Abortable to catch it and try to create a new session with ZooKeeper. This is what the client does in HCM.
     * <p>
     * 
     * @param event
     */
    private void connectionEvent(WatchedEvent event) {
        switch (event.getState()) {
        case SyncConnected:// Now, this callback can be invoked before the this.zookeeper is set.Wait a little while.
            waitUntilReady();
            this.identifier = this.identifier + "-0x" + Long.toHexString(this.recoverableZooKeeper.getSessionId());
            // Update our identifier. Otherwise ignore.
            LOG.debug(this.identifier + " connected");
            break;
        // Abort the server if Disconnected or Expired
        case Disconnected:
            LOG.debug(prefix("Received Disconnected from ZooKeeper, ignoring"));
            break;
        case Expired:
            String msg = prefix(this.identifier + " received expired from ZooKeeper, aborting");
            // One thought is to add call to ZooKeeperListener so say, ZooKeeperNodeTracker can zero out its data values.
            if (this.abortable != null) {
                this.abortable.abort(msg, new KeeperException.SessionExpiredException());
            }
            break;
        default:
        }
    }

    private void waitUntilReady() {
        long finished = System.currentTimeMillis() + this.conf.getLong("hbase.zookeeper.watcher.sync.connected.wait", 2000);
        while (System.currentTimeMillis() < finished) {
            Threads.sleep(1);
            if (this.recoverableZooKeeper != null) {
                break;
            }
        }
        if (this.recoverableZooKeeper == null) {
            LOG.error("ZK is null on connection event -- see stack trace for the stack trace when constructor was called on this zkw", this.constructorCaller);
            throw new NullPointerException("ZK is null");
        }
    }

    /**
     * Handles KeeperExceptions in client calls.
     * <p>
     * This may be temporary but for now this gives one place to deal with these.
     * <p>
     * TO DO: Currently this method rethrows the exception to let the caller handle
     * <p>
     * 
     * @param ke
     * @throws KeeperException
     */
    public void keeperException(KeeperException ke) throws KeeperException {
        LOG.error(prefix("Received unexpected KeeperException, re-throwing exception"), ke);
        throw ke;
    }

    /**
     * Handles InterruptedExceptions in client calls.
     * <p>
     * This may be temporary but for now this gives one place to deal with these.
     * <p>
     * TO DO: Currently, this method does nothing. Is this ever expected to happen? Do we abort or can we let it run? Maybe this should be logged as WARN? It shouldn't happen?
     * <p>
     * 
     * @param ie
     */
    public void interruptedException(InterruptedException ie) {
        LOG.debug(prefix("Received InterruptedException, doing nothing here"), ie);
        // At least preserver interrupt.
        Thread.currentThread().interrupt();
        // no-op
    }

    /**
     * Close the connection to ZooKeeper.
     * 
     * @throws InterruptedException
     */
    public void close() {
        try {
            if (recoverableZooKeeper != null) {
                recoverableZooKeeper.close();
            }
        } catch (InterruptedException e) {
            LOG.info(e.getMessage(), e);
        }
    }

    public Configuration getConfiguration() {
        return conf;
    }

    @Override
    public void abort(String why, Throwable e) {
        this.abortable.abort(why, e);
    }

    @Override
    public boolean isAborted() {
        return this.abortable.isAborted();
    }

    public String getRootZNode() {
        return rootZNode;
    }

    public void setRootZNode(String rootZNode) {
        this.rootZNode = rootZNode;
    }
}