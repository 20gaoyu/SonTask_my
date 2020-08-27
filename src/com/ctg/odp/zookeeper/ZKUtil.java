package com.ctg.odp.zookeeper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;

import com.ctg.odp.zookeeper.ZKUtil.ZKUtilOp.CreateAndFailSilent;
import com.ctg.odp.zookeeper.ZKUtil.ZKUtilOp.DeleteNodeFailSilent;
import com.ctg.odp.zookeeper.ZKUtil.ZKUtilOp.SetData;

/**
 * Internal utility class for ZooKeeper.
 * 
 */
public class ZKUtil {
	private static final Log LOG = LogFactory.getLog(ZKUtil.class);

	private static final char ZNODE_PATH_SEPARATOR = '/';

	private ZKUtil() {
	}

	public static RecoverableZooKeeper connect(Configuration conf, Watcher watcher) throws IOException {
		String quorumServers = getQuorumServersString(conf);
		return connect(conf, quorumServers, watcher);
	}

	public static RecoverableZooKeeper connect(Configuration conf, String quorumServers, Watcher watcher) throws IOException {
		return connect(conf, quorumServers, watcher, "");
	}

	public static RecoverableZooKeeper connect(Configuration conf, String quorumServers, Watcher watcher, final String descriptor) throws IOException {
		if (quorumServers == null) {
			throw new IOException("Unable to determine ZooKeeper querumServers");
		}
		int sessionTimeout = conf.getInt(OdpConstants.ODP_ZOOKEEPER_SESSION_SESSIONTIMEOUT, OdpConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
		LOG.debug(descriptor + " opening connection to ZooKeeper with querumServers (" + quorumServers + ")");
		int retries = conf.getInt(OdpConstants.ODP_ZOOKEEPER_RECOVERY_RETRIES, OdpConstants.DEFAULT_ZOOKEEPER_RECOVERABLE_RETRIES);

		int retryInterval = conf.getInt(OdpConstants.ODP_ZOOKEEPER_RECOVERY_INTERVAL, OdpConstants.DEFAULT_ZOOKEEPER_RECOVERABLE_INTERVAL);
		return new RecoverableZooKeeper(quorumServers, sessionTimeout, watcher, retries, retryInterval);
	}

	public static String getQuorumServersString(Configuration conf) {
		return conf.get(OdpConstants.ODP_ZOOKEEPER_QUORUMSERVERS, OdpConstants.DEFAULT_ZOOKEPER_QUORUMSERVERS);
	}

	public static String getZNodeRoot(Configuration conf) {
		return conf.get(OdpConstants.ODP_ZOOKEEPER_ZNODE_ROOT, OdpConstants.DEFAULT_ZOOKEEPER_ZNODE_ROOT);
	}

	//
	// Helper methods
	//

	/**
	 * Join the prefix parent name with the suffix znode name to generate a
	 * proper full znode name.
	 * 
	 * Assumes prefix does not end with slash and suffix does not begin with it.
	 * 
	 * @param parent
	 *            beginning of znode name
	 * @param znode
	 *            ending of znode name
	 * @return result of properly joining prefix with suffix
	 */
	public static String joinZNode(String parent, String znode) {
		return parent + ZNODE_PATH_SEPARATOR + znode;
	}

	/**
	 * Returns the full path of the immediate parent of the specified node.
	 * 
	 * @param node
	 *            path to get parent of
	 * @return parent of path, null if passed the root node or an invalid node
	 */
	public static String getParent(String node) {
		int idx = node.lastIndexOf(ZNODE_PATH_SEPARATOR);
		return idx <= 0 ? null : node.substring(0, idx);
	}

	/**
	 * Get the name of the current node from the specified fully-qualified path.
	 * 
	 * @param path
	 *            fully-qualified path
	 * @return name of the current node
	 */
	public static String getNodeName(String path) {
		return path.substring(path.lastIndexOf("/") + 1);
	}

	/**
	 * Watch the specified znode for delete/create/change events. The watcher is
	 * set whether or not the node exists. If the node already exists, the
	 * method returns true. If the node does not exist, the method returns
	 * false.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to watch
	 * @return true if znode exists, false if does not exist or error
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static boolean watchAndCheckExists(ZooKeeperWatcher zkw, String znode) throws KeeperException {
		try {
			Stat s = zkw.getRecoverableZooKeeper().exists(znode, zkw);
			boolean exists = s != null ? true : false;
			if (exists) {
				LOG.debug(zkw.prefix("Set watcher on existing znode " + znode));
			} else {
				LOG.debug(zkw.prefix(znode + " does not exist. Watcher is set."));
			}
			return exists;
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
			zkw.keeperException(e);
			return false;
		} catch (InterruptedException e) {
			LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
			zkw.interruptedException(e);
			return false;
		}
	}

	/**
	 * Check if the specified node exists. Sets no watches.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to watch
	 * @return version of the node if it exists, -1 if does not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static int checkExists(ZooKeeperWatcher zkw, String znode) throws KeeperException {
		try {
			Stat s = zkw.getRecoverableZooKeeper().exists(znode, null);
			return s != null ? s.getVersion() : -1;
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix("Unable to set watcher on znode (" + znode + ")"), e);
			zkw.keeperException(e);
			return -1;
		} catch (InterruptedException e) {
			LOG.warn(zkw.prefix("Unable to set watcher on znode (" + znode + ")"), e);
			zkw.interruptedException(e);
			return -1;
		}
	}

	//
	// Znode listings
	//

	/**
	 * Lists the children znodes of the specified znode. Also sets a watch on
	 * the specified znode which will capture a NodeDeleted event on the
	 * specified znode as well as NodeChildrenChanged if any children of the
	 * specified znode are created or deleted.
	 * 
	 * Returns null if the specified node does not exist. Otherwise returns a
	 * list of children of the specified node. If the node exists but it has no
	 * children, an empty list will be returned.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to list and watch children of
	 * @return list of children of the specified node, an empty list if the node
	 *         exists but has no children, and null if the node does not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static List<String> listChildrenAndWatchForNewChildren(ZooKeeperWatcher zkw, String znode) throws KeeperException {
		String failedPrefix = "Unable to list children of znode " + znode;
		try {
			return zkw.getRecoverableZooKeeper().getChildren(znode, zkw);
		} catch (KeeperException.NoNodeException e) {
			LOG.info(zkw.prefix(failedPrefix + ",because node does not exist (not an error)"), e);
			return Collections.emptyList();
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix(failedPrefix), e);
			zkw.keeperException(e);
			return Collections.emptyList();
		} catch (InterruptedException e) {
			LOG.warn(zkw.prefix(failedPrefix), e);
			zkw.interruptedException(e);
			return Collections.emptyList();
		}
	}

	/**
	 * List all the children of the specified znode, setting a watch for
	 * children changes and also setting a watch on every individual child in
	 * order to get the NodeCreated and NodeDeleted events.
	 * 
	 * @param zkw
	 *            zookeeper reference
	 * @param znode
	 *            node to get children of and watch
	 * @return list of znode names, null if the node doesn't exist
	 * @throws KeeperException
	 */
	public static List<String> listChildrenAndWatchThem(ZooKeeperWatcher zkw, String znode) throws KeeperException {
		LOG.info("get child:" + znode);
		List<String> children = listChildrenAndWatchForNewChildren(zkw, znode);
		if (children == null) {
			return new ArrayList<String>();
		}
		for (String child : children) {
			watchAndCheckExists(zkw, joinZNode(znode, child));
		}
		return children;
	}

	/**
	 * Lists the children of the specified znode without setting any watches.
	 * 
	 * Used to list the currently online regionservers and their addresses.
	 * 
	 * Sets no watches at all, this method is best effort.
	 * 
	 * Returns an empty list if the node has no children. Returns null if the
	 * parent node itself does not exist.
	 * 
	 * @param zkw
	 *            zookeeper reference
	 * @param znode
	 *            node to get children of as addresses
	 * @return list of data of children of specified znode, empty if no
	 *         children, null if parent does not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static List<String> listChildrenNoWatch(ZooKeeperWatcher zkw, String znode) throws KeeperException {
		try {
			// List the children without watching
			return zkw.getRecoverableZooKeeper().getChildren(znode, null);
		} catch (KeeperException.NoNodeException nne) {
			LOG.warn(nne.getMessage(), nne);
			return Collections.emptyList();
		} catch (InterruptedException ie) {
			LOG.warn(ie.getMessage(), ie);
			zkw.interruptedException(ie);
			return Collections.emptyList();
		}
	}

	/**
	 * Simple class to hold a node path and node data.
	 */
	public static class NodeAndData {
		private final String node;
		private final byte[] data;

		public NodeAndData(String node, byte[] data) {
			this.node = node;
			this.data = data;
		}

		public String getNode() {
			return node;
		}

		public byte[] getData() {
			return data;
		}

		@Override
		public String toString() {
			return node + " (" + new String(data) + ")";
		}

		public boolean isEmpty() {
			return data.length == 0;
		}
	}

	/**
	 * Checks if the specified znode has any children. Sets no watches.
	 * 
	 * Returns true if the node exists and has children. Returns false if the
	 * node does not exist or if the node does not have any children.
	 * 
	 * Used during master initialization to determine if the master is a
	 * failed-over-to master or the first master during initial cluster startup.
	 * If the directory for regionserver ephemeral nodes is empty then this is a
	 * cluster startup, if not then it is not cluster startup.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to check for children of
	 * @return true if node has children, false if not or node does not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static boolean nodeHasChildren(ZooKeeperWatcher zkw, String znode) throws KeeperException {
		return !listChildrenNoWatch(zkw, znode).isEmpty();
	}

	/**
	 * Get the number of children of the specified node.
	 * 
	 * If the node does not exist or has no children, returns 0.
	 * 
	 * Sets no watches at all.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to count children of
	 * @return number of children of specified node, 0 if none or parent does
	 *         not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static int getNumberOfChildren(ZooKeeperWatcher zkw, String znode) throws KeeperException {
		try {
			Stat stat = zkw.getRecoverableZooKeeper().exists(znode, null);
			return stat == null ? 0 : stat.getNumChildren();
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix("Unable to get children of node " + znode));
			zkw.keeperException(e);
		} catch (InterruptedException e) {
			zkw.interruptedException(e);
		}
		return 0;
	}

	private static byte[] getDataInternal(ZooKeeperWatcher zkw, String znode, Stat stat, boolean watcherSet) throws KeeperException {
		String failedMessagePrefix = "Unable to get data of znode ";
		try {
			if (watcherSet) {
				byte[] data = zkw.getRecoverableZooKeeper().getData(znode, zkw, stat);
				logRetrievedMsg(zkw, znode, data, watcherSet);
				return data;
			} else {
				byte[] data = zkw.getRecoverableZooKeeper().getData(znode, null, stat);
				logRetrievedMsg(zkw, znode, data, watcherSet);
				return data;
			}
		} catch (KeeperException.NoNodeException e) {
			LOG.debug(zkw.prefix(failedMessagePrefix + znode + " " + "because node does not exist (not an error)"), e);
			return new byte[0];
		} catch (KeeperException e) {
			LOG.warn(zkw.prefix(failedMessagePrefix + znode), e);
			zkw.keeperException(e);
			return new byte[0];
		} catch (InterruptedException e) {
			LOG.warn(zkw.prefix(failedMessagePrefix + znode), e);
			zkw.interruptedException(e);
			return new byte[0];
		}
	}

	/**
	 * Get the data at the specified znode and set a watch.
	 * 
	 * Returns the data and sets a watch if the node exists. Returns null and no
	 * watch is set if the node does not exist or there is an exception.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @return data of the specified znode, or null
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static byte[] getDataAndWatch(ZooKeeperWatcher zkw, String znode) throws KeeperException {
		return getDataInternal(zkw, znode, null, true);
	}

	/**
	 * Get the data at the specified znode and set a watch.
	 * 
	 * Returns the data and sets a watch if the node exists. Returns null and no
	 * watch is set if the node does not exist or there is an exception.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param stat
	 *            object to populate the version of the znode
	 * @return data of the specified znode, or null
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static byte[] getDataAndWatch(ZooKeeperWatcher zkw, String znode, Stat stat) throws KeeperException {
		return getDataInternal(zkw, znode, stat, true);
	}

	/**
	 * Get znode data. Does not set a watcher.
	 * 
	 * @return ZNode data
	 */
	public static byte[] getData(ZooKeeperWatcher zkw, String znode) throws KeeperException {
		return getDataInternal(zkw, znode, null, false);
	}

	/**
	 * Get the data at the specified znode without setting a watch.
	 * 
	 * Returns the data if the node exists. Returns null if the node does not
	 * exist.
	 * 
	 * Sets the stats of the node in the passed Stat object. Pass a null stat if
	 * not interested.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param stat
	 *            node status to get if node exists
	 * @return data of the specified znode, or null if node does not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static byte[] getDataNoWatch(ZooKeeperWatcher zkw, String znode, Stat stat) throws KeeperException {
		return getDataInternal(zkw, znode, stat, false);
	}

	/**
	 * Returns the date of child znodes of the specified znode. Also sets a
	 * watch on the specified znode which will capture a NodeDeleted event on
	 * the specified znode as well as NodeChildrenChanged if any children of the
	 * specified znode are created or deleted.
	 * 
	 * Returns null if the specified node does not exist. Otherwise returns a
	 * list of children of the specified node. If the node exists but it has no
	 * children, an empty list will be returned.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param baseNode
	 *            path of node to list and watch children of
	 * @return list of data of children of the specified node, an empty list if
	 *         the node exists but has no children, and null if the node does
	 *         not exist
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static List<NodeAndData> getChildDataAndWatchForNewChildren(ZooKeeperWatcher zkw, String baseNode) throws KeeperException {
		List<String> nodes = ZKUtil.listChildrenAndWatchForNewChildren(zkw, baseNode);
		List<NodeAndData> newNodes = new ArrayList<NodeAndData>();
		if (nodes != null) {
			for (String node : nodes) {
				String nodePath = ZKUtil.joinZNode(baseNode, node);
				byte[] data = ZKUtil.getDataAndWatch(zkw, nodePath);
				newNodes.add(new NodeAndData(nodePath, data));
			}
		}
		return newNodes;
	}

	/**
	 * Update the data of an existing node with the expected version to have the
	 * specified data.
	 * 
	 * Throws an exception if there is a version mismatch or some other problem.
	 * 
	 * Sets no watches under any conditions.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 * @param data
	 * @param expectedVersion
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 * @throws KeeperException.BadVersionException
	 *             if version mismatch
	 */
	public static void updateExistingNodeData(ZooKeeperWatcher zkw, String znode, byte[] data, int expectedVersion) throws KeeperException {
		try {
			zkw.getRecoverableZooKeeper().setData(znode, data, expectedVersion);
		} catch (InterruptedException ie) {
			LOG.warn(ie.getMessage(), ie);
			zkw.interruptedException(ie);
		}
	}

	//
	// Data setting
	//

	/**
	 * Sets the data of the existing znode to be the specified data. Ensures
	 * that the current data has the specified expected version.
	 * 
	 * <p>
	 * If the node does not exist, a {@link NoNodeException} will be thrown.
	 * 
	 * <p>
	 * If their is a version mismatch, method returns null.
	 * 
	 * <p>
	 * No watches are set but setting data will trigger other watchers of this
	 * node.
	 * 
	 * <p>
	 * If there is another problem, a KeeperException will be thrown.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            data to set for node
	 * @param expectedVersion
	 *            version expected when setting data
	 * @return true if data set, false if version mismatch
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static boolean setData(ZooKeeperWatcher zkw, String znode, byte[] data, int expectedVersion) throws KeeperException {
		try {
			return zkw.getRecoverableZooKeeper().setData(znode, data, expectedVersion) != null;
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
			zkw.interruptedException(e);
			return false;
		}
	}

	/**
	 * Set data into node creating node if it doesn't yet exist. Does not set
	 * watch.
	 * 
	 * WARNING: this is not atomic -- it is possible to get a 0-byte data value
	 * in the znode before data is written
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            data to set for node
	 * @throws KeeperException
	 */
	public static void createSetData(final ZooKeeperWatcher zkw, final String znode, final byte[] data) throws KeeperException {
		if (checkExists(zkw, znode) == -1) {
			ZKUtil.createWithParents(zkw, znode);
		}
		ZKUtil.setData(zkw, znode, data);
	}

	/**
	 * Sets the data of the existing znode to be the specified data. The node
	 * must exist but no checks are done on the existing data or version.
	 * 
	 * <p>
	 * If the node does not exist, a {@link NoNodeException} will be thrown.
	 * 
	 * <p>
	 * No watches are set but setting data will trigger other watchers of this
	 * node.
	 * 
	 * <p>
	 * If there is another problem, a KeeperException will be thrown.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            data to set for node
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static void setData(ZooKeeperWatcher zkw, String znode, byte[] data) throws KeeperException {
		setData(zkw, (SetData) ZKUtilOp.setData(znode, data));
	}

	private static void setData(ZooKeeperWatcher zkw, SetData setData) throws KeeperException {
		SetDataRequest sd = (SetDataRequest) toZooKeeperOp(zkw, setData).toRequestRecord();
		setData(zkw, sd.getPath(), sd.getData(), sd.getVersion());
	}

	/**
	 * Returns whether or not secure authentication is enabled (whether
	 * <code>hbase.security.authentication</code> is set to
	 * <code>kerberos</code>.
	 */
	public static boolean isSecureZooKeeper(Configuration conf) {
		// hbase shell need to use:
		// -Djava.security.auth.login.config=user-jaas.conf
		// since each user has a different jaas.conf
		if (System.getProperty("java.security.auth.login.config") != null)
			return true;

		// Master & RSs uses hbase.zookeeper.client.*
		return "kerberos".equalsIgnoreCase(conf.get("hbase.security.authentication")) && conf.get("hbase.zookeeper.client.keytab.file") != null;
	}

	/*
	 * 鎸夌収瀹㈡埛绔拰鏈嶅姟绔垎寮�鎺у埗鏉冮檺锛� 瀹㈡埛绔細CREATOR_ALL_AND_WORLD_READABLE 鏈嶅姟绔細CREATOR_ALL_ACL
	 */
	private static List<ACL> createACL(ZooKeeperWatcher zkw, String node) {
		if (isSecureZooKeeper(zkw.getConfiguration())) {
			// Certain znodes are accessed directly by the client,so they must
			// be readable by non-authenticated clients
			if (node.equals(zkw.getRootZNode())) {
				return ZooKeeperWatcher.CREATOR_ALL_AND_WORLD_READABLE;
			}
			return Ids.CREATOR_ALL_ACL;
		} else {
			return Ids.OPEN_ACL_UNSAFE;
		}
	}

	//
	// Node creation
	//

	/**
	 * 
	 * Set the specified znode to be an ephemeral node carrying the specified
	 * data.
	 * 
	 * If the node is created successfully, a watcher is also set on the node.
	 * 
	 * If the node is not created successfully because it already exists, this
	 * method will also set a watcher on the node.
	 * 
	 * If there is another problem, a KeeperException will be thrown.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            data of node
	 * @return true if node created, false if not, watch set in both cases
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static boolean createEphemeralNodeAndWatch(ZooKeeperWatcher zkw, String znode, byte[] data) throws KeeperException {
		try {
			zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode), CreateMode.EPHEMERAL);
		} catch (KeeperException.NodeExistsException nee) {
			LOG.info("KeeperException.NodeExistsException:" + nee.getMessage(), nee);
			if (!watchAndCheckExists(zkw, znode)) {
				// It did exist but now it doesn't, try again
				return createEphemeralNodeAndWatch(zkw, znode, data);
			}
			return false;
		} catch (InterruptedException e) {
			LOG.info("Interrupted", e);
			Thread.currentThread().interrupt();
		}
		return true;
	}

	/**
	 * Creates the specified znode to be a persistent node carrying the
	 * specified data.
	 * 
	 * Returns true if the node was successfully created, false if the node
	 * already existed.
	 * 
	 * If the node is created successfully, a watcher is also set on the node.
	 * 
	 * If the node is not created successfully because it already exists, this
	 * method will also set a watcher on the node but return false.
	 * 
	 * If there is another problem, a KeeperException will be thrown.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            data of node
	 * @return true if node created, false if not, watch set in both cases
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static boolean createNodeIfNotExistsAndWatch(ZooKeeperWatcher zkw, String znode, byte[] data) throws KeeperException {
		try {
			zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode), CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException nee) {
			LOG.info("KeeperException.NodeExistsException:" + nee.getMessage(), nee);
			try {
				zkw.getRecoverableZooKeeper().exists(znode, zkw);
			} catch (InterruptedException e) {
				LOG.warn(e.getMessage(), e);
				zkw.interruptedException(e);
				return false;
			}
			return false;
		} catch (InterruptedException e) {
			LOG.warn(e.getMessage(), e);
			zkw.interruptedException(e);
			return false;
		}
		return true;
	}

	/**
	 * Creates the specified node with the specified data and watches it.
	 * 
	 * <p>
	 * Throws an exception if the node already exists.
	 * 
	 * <p>
	 * The node created is persistent and open access.
	 * 
	 * <p>
	 * Returns the version number of the created node if successful.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to create
	 * @param data
	 *            data of node to create
	 * @return version of node created
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 * @throws KeeperException.NodeExistsException
	 *             if node already exists
	 */
	public static int createAndWatch(ZooKeeperWatcher zkw, String znode, byte[] data) throws KeeperException {
		try {
			zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode), CreateMode.PERSISTENT);
			return zkw.getRecoverableZooKeeper().exists(znode, zkw).getVersion();
		} catch (InterruptedException e) {
			LOG.warn(e.getMessage(), e);
			zkw.interruptedException(e);
			return -1;
		}
	}

	/**
	 * Async creates the specified node with the specified data.
	 * 
	 * <p>
	 * Throws an exception if the node already exists.
	 * 
	 * <p>
	 * The node created is persistent and open access.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node to create
	 * @param data
	 *            data of node to create
	 * @param cb
	 * @param ctx
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 * @throws KeeperException.NodeExistsException
	 *             if node already exists
	 */
	public static void asyncCreate(ZooKeeperWatcher zkw, String znode, byte[] data, final AsyncCallback.StringCallback cb, final Object ctx) {
		zkw.getRecoverableZooKeeper().getZooKeeper().create(znode, data, createACL(zkw, znode), CreateMode.PERSISTENT, cb, ctx);
	}

	/**
	 * Creates the specified node, iff the node does not exist. Does not set a
	 * watch and fails silently if the node already exists.
	 * 
	 * The node created is persistent and open access.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static void createAndFailSilent(ZooKeeperWatcher zkw, String znode) throws KeeperException {
		createAndFailSilent(zkw, znode, new byte[0]);
	}

	/**
	 * Creates the specified node containing specified data, iff the node does
	 * not exist. Does not set a watch and fails silently if the node already
	 * exists.
	 * 
	 * The node created is persistent and open access.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @param data
	 *            a byte array data to store in the znode
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static void createAndFailSilent(ZooKeeperWatcher zkw, String znode, byte[] data) throws KeeperException {
		createAndFailSilent(zkw, (CreateAndFailSilent) ZKUtilOp.createAndFailSilent(znode, data));
	}

	private static void createAndFailSilent(ZooKeeperWatcher zkw, CreateAndFailSilent cafs) throws KeeperException {
		CreateRequest create = (CreateRequest) toZooKeeperOp(zkw, cafs).toRequestRecord();
		String znode = create.getPath();
		try {
			RecoverableZooKeeper zk = zkw.getRecoverableZooKeeper();
			if (zk.exists(znode, false) == null) {
				zk.create(znode, create.getData(), create.getAcl(), CreateMode.fromFlag(create.getFlags()));
			}
		} catch (KeeperException.NodeExistsException nee) {
			LOG.warn(nee.getMessage(), nee);
		} catch (KeeperException.NoAuthException nee) {
			try {
				if (null == zkw.getRecoverableZooKeeper().exists(znode, false)) {
					// If we failed to create the file and it does not already
					// exist.
					throw nee;
				}
			} catch (InterruptedException ie) {
				zkw.interruptedException(ie);
			}

		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
		}
	}

	/**
	 * Creates the specified node and all parent nodes required for it to exist.
	 * 
	 * No watches are set and no errors are thrown if the node already exists.
	 * 
	 * The nodes created are persistent and open access.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static void createWithParents(ZooKeeperWatcher zkw, String znode) throws KeeperException {
		createWithParents(zkw, znode, new byte[0]);
	}

	/**
	 * Creates the specified node and all parent nodes required for it to exist.
	 * The creation of parent znodes is not atomic with the leafe znode creation
	 * but the data is written atomically when the leaf node is created.
	 * 
	 * No watches are set and no errors are thrown if the node already exists.
	 * 
	 * The nodes created are persistent and open access.
	 * 
	 * @param zkw
	 *            zk reference
	 * @param znode
	 *            path of node
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public static void createWithParents(ZooKeeperWatcher zkw, String znode, byte[] data) throws KeeperException {
		if (znode == null) {
			return;
		}
		try {
			zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode), CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException nee) {
			LOG.warn(nee.getMessage(), nee);
			return;
		} catch (KeeperException.NoNodeException nne) {
			LOG.warn(nne.getMessage(), nne);
			createWithParents(zkw, getParent(znode));
			createWithParents(zkw, znode);
		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
		}
	}

	//
	// Deletes
	//

	/**
	 * Delete the specified node. Sets no watches. Throws all exceptions.
	 */
	public static void deleteNode(ZooKeeperWatcher zkw, String node) throws KeeperException {
		deleteNode(zkw, node, -1);
	}

	/**
	 * Delete the specified node with the specified version. Sets no watches.
	 * Throws all exceptions.
	 */
	public static boolean deleteNode(ZooKeeperWatcher zkw, String node, int version) throws KeeperException {
		try {
			zkw.getRecoverableZooKeeper().delete(node, version);
			return true;
		} catch (KeeperException.BadVersionException bve) {
			LOG.warn(bve.getMessage(), bve);
			return false;
		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
			return false;
		}
	}

	/**
	 * Deletes the specified node. Fails silent if the node does not exist.
	 * 
	 * @param zkw
	 * @param node
	 * @throws KeeperException
	 */
	public static void deleteNodeFailSilent(ZooKeeperWatcher zkw, String node) throws KeeperException {
		deleteNodeFailSilent(zkw, (DeleteNodeFailSilent) ZKUtilOp.deleteNodeFailSilent(node));
	}

	private static void deleteNodeFailSilent(ZooKeeperWatcher zkw, DeleteNodeFailSilent dnfs) throws KeeperException {
		DeleteRequest delete = (DeleteRequest) toZooKeeperOp(zkw, dnfs).toRequestRecord();
		try {
			zkw.getRecoverableZooKeeper().delete(delete.getPath(), delete.getVersion());
		} catch (KeeperException.NoNodeException nne) {
			LOG.warn(nne.getMessage(), nne);
		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
		}
	}

	/**
	 * Delete the specified node and all of it's children.
	 * <p>
	 * If the node does not exist, just returns.
	 * <p>
	 * Sets no watches. Throws all exceptions besides dealing with deletion of
	 * children.
	 */
	public static void deleteNodeRecursively(ZooKeeperWatcher zkw, String node) throws KeeperException {
		try {
			List<String> children = ZKUtil.listChildrenNoWatch(zkw, node);
			if (children != null && !children.isEmpty()) {
				for (String child : children) {
					deleteNodeRecursively(zkw, joinZNode(node, child));
				}
			}
			zkw.getRecoverableZooKeeper().delete(node, -1);
		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
		}
	}

	/**
	 * Delete all the children of the specified node but not the node itself.
	 * 
	 * Sets no watches. Throws all exceptions besides dealing with deletion of
	 * children.
	 */
	public static void deleteChildrenRecursively(ZooKeeperWatcher zkw, String node) throws KeeperException {
		List<String> children = ZKUtil.listChildrenNoWatch(zkw, node);
		if (children == null || children.isEmpty())
			return;
		for (String child : children) {
			deleteNodeRecursively(zkw, joinZNode(node, child));
		}
	}

	/**
	 * Represents an action taken by ZKUtil, e.g. createAndFailSilent. These
	 * actions are higher-level than {@link ZKOp} actions, which represent
	 * individual actions in the ZooKeeper API, like create.
	 */
	public abstract static class ZKUtilOp {
		private final String path;

		private ZKUtilOp(String path) {
			this.path = path;
		}

		/**
		 * @return a createAndFailSilent ZKUtilOp
		 */
		public static ZKUtilOp createAndFailSilent(String path, byte[] data) {
			return new CreateAndFailSilent(path, data);
		}

		/**
		 * @return a deleteNodeFailSilent ZKUtilOP
		 */
		public static ZKUtilOp deleteNodeFailSilent(String path) {
			return new DeleteNodeFailSilent(path);
		}

		/**
		 * @return a setData ZKUtilOp
		 */
		public static ZKUtilOp setData(String path, byte[] data) {
			return new SetData(path, data);
		}

		/**
		 * @return path to znode where the ZKOp will occur
		 */
		public String getPath() {
			return path;
		}

		/**
		 * ZKUtilOp representing createAndFailSilent in ZooKeeper (attempt to
		 * create node, ignore error if already exists)
		 */
		public static class CreateAndFailSilent extends ZKUtilOp {
			private final byte[] data1;

			private CreateAndFailSilent(String path, byte[] data) {
				super(path);
				this.data1 = data;
			}

			public byte[] getData() {
				return data1;
			}

			@Override
			public int hashCode() {
				final int prime = 31;
				int result = 1;
				result = prime * result + Arrays.hashCode(data1);
				return result;
			}

			@Override
			public boolean equals(Object obj) {
				if (this == obj)
					return true;
				if (obj == null)
					return false;
				if (getClass() != obj.getClass())
					return false;
				CreateAndFailSilent other = (CreateAndFailSilent) obj;
				if (!Arrays.equals(data1, other.data1))
					return false;
				return true;
			}
		}

		/**
		 * ZKUtilOp representing deleteNodeFailSilent in ZooKeeper (attempt to
		 * delete node, ignore error if node doesn't exist)
		 */
		public static class DeleteNodeFailSilent extends ZKUtilOp {
			private DeleteNodeFailSilent(String path) {
				super(path);
			}

			@Override
			public boolean equals(Object o) {
				if (this == o)
					return true;
				if (!(o instanceof DeleteNodeFailSilent))
					return false;

				return super.equals(o);
			}

			@Override
			public int hashCode() {
				return this.getPath().hashCode();
			}
		}

		/**
		 * @return ZKUtilOp representing setData in ZooKeeper
		 */
		public static class SetData extends ZKUtilOp {
			private final byte[] data2;

			private SetData(String path, byte[] data) {
				super(path);
				this.data2 = data;
			}

			public byte[] getData() {
				return data2;
			}

			@Override
			public int hashCode() {
				final int prime = 31;
				int result = 1;
				result = prime * result + Arrays.hashCode(data2);
				return result;
			}

			@Override
			public boolean equals(Object o) {
				if (this == o)
					return true;
				if (!(o instanceof SetData))
					return false;

				SetData op = (SetData) o;
				return getPath().equals(op.getPath()) && Arrays.equals(data2, op.data2);
			}
		}
	}

	/**
	 * Convert from ZKUtilOp to ZKOp
	 */
	private static Op toZooKeeperOp(ZooKeeperWatcher zkw, ZKUtilOp op) {
		if (op == null)
			return null;

		if (op instanceof CreateAndFailSilent) {
			CreateAndFailSilent cafs = (CreateAndFailSilent) op;
			return Op.create(cafs.getPath(), cafs.getData(), createACL(zkw, cafs.getPath()), CreateMode.PERSISTENT);
		} else if (op instanceof DeleteNodeFailSilent) {
			DeleteNodeFailSilent dnfs = (DeleteNodeFailSilent) op;
			return Op.delete(dnfs.getPath(), -1);
		} else if (op instanceof SetData) {
			SetData sd = (SetData) op;
			return Op.setData(sd.getPath(), sd.getData(), -1);
		} else {
			throw new UnsupportedOperationException("Unexpected ZKUtilOp type: " + op.getClass().getName());
		}
	}

	/**
	 * If hbase.zookeeper.useMulti is true, use ZooKeeper's multi-update
	 * functionality. Otherwise, run the list of operations sequentially.
	 * 
	 * If all of the following are true: - runSequentialOnMultiFailure is true -
	 * hbase.zookeeper.useMulti is true - on calling multi, we get a ZooKeeper
	 * exception that can be handled by a sequential call(*) Then: - we retry
	 * the operations one-by-one (sequentially)
	 * 
	 * Note *: an example is receiving a NodeExistsException from a "create"
	 * call. Without multi, a user could call "createAndFailSilent" to ensure
	 * that a node exists if they don't care who actually created the node (i.e.
	 * the NodeExistsException from ZooKeeper is caught). This will cause all
	 * operations in the multi to fail, however, because the NodeExistsException
	 * that zk.create throws will fail the multi transaction. In this case, if
	 * the previous conditions hold, the commands are run sequentially, which
	 * should result in the correct final state, but means that the operations
	 * will not run atomically.
	 * 
	 * @throws KeeperException
	 */
	public static void multiOrSequential(ZooKeeperWatcher zkw, List<ZKUtilOp> ops, boolean runSequentialOnMultiFailure) throws KeeperException {
		if (ops == null)
			return;
		boolean useMulti = zkw.getConfiguration().getBoolean("anypaas.zookeeper.useMulti", false);

		if (!useMulti) {
			processSequentially(zkw, ops);
			return;
		}
		List<Op> zkOps = ops.stream().map(op -> ZKUtil.toZooKeeperOp(zkw, op)).collect(Collectors.toList());
		try {
			zkw.getRecoverableZooKeeper().multi(zkOps);
		} catch (KeeperException ke) {
			tryProcessSequentially(zkw, ops, runSequentialOnMultiFailure, ke);
		} catch (InterruptedException ie) {
			zkw.interruptedException(ie);
		}
	}

	private static void tryProcessSequentially(ZooKeeperWatcher zkw, List<ZKUtilOp> ops, boolean runSequentialOnMultiFailure, KeeperException ke)
			throws KeeperException {
		String format = "On call to ZK.multi, received exception: %s. Attempting to run operations sequentially because runSequentialOnMultiFailure is: %s.";
		switch (ke.code()) {
		case NODEEXISTS:
		case NONODE:
		case BADVERSION:
		case NOAUTH:
			if (runSequentialOnMultiFailure) {
				LOG.info(String.format(format, ke.toString(), String.valueOf(runSequentialOnMultiFailure)));
				processSequentially(zkw, ops);
				break;
			}
		default:
			throw ke;
		}
	}

	private static void processSequentially(ZooKeeperWatcher zkw, List<ZKUtilOp> ops) throws KeeperException {
		for (ZKUtilOp op : ops) {
			if (op instanceof CreateAndFailSilent) {
				createAndFailSilent(zkw, (CreateAndFailSilent) op);
			} else if (op instanceof DeleteNodeFailSilent) {
				deleteNodeFailSilent(zkw, (DeleteNodeFailSilent) op);
			} else if (op instanceof SetData) {
				setData(zkw, (SetData) op);
			} else {
				throw new UnsupportedOperationException("Unexpected ZKUtilOp type: " + op.getClass().getName());
			}
		}
	}

	/**
	 * Gets the statistics from the given server.
	 * 
	 * @param server
	 *            The server to get the statistics from.
	 * @param timeout
	 *            The socket timeout to use.
	 * @return The array of response strings.
	 * @throws IOException
	 *             When the socket communication fails.
	 */
	public static String[] getServerStats(String server, int timeout) throws IOException {
		String[] sp = server.split(":");
		if (sp == null || sp.length == 0) {
			return new String[0];
		}

		String host = sp[0];
		int port = sp.length > 1 ? Integer.parseInt(sp[1]) : OdpConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;

		Socket socket = new Socket();
		InetSocketAddress sockAddr = new InetSocketAddress(host, port);
		socket.connect(sockAddr, timeout);

		socket.setSoTimeout(timeout);
		PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
		BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		out.println("stat");
		out.flush();
		List<String> res = new ArrayList<String>();
		while (true) {
			String line = in.readLine();
			if (line != null) {
				res.add(line);
			} else {
				break;
			}
		}
		socket.close();
		return res.toArray(new String[res.size()]);
	}

	private static void logRetrievedMsg(final ZooKeeperWatcher zkw, final String znode, final byte[] data, final boolean watcherSet) {
		if (!LOG.isDebugEnabled())
			return;
		LOG.debug(zkw.prefix("Retrieved " + ((data == null) ? 0 : data.length) + " byte(s) of data from znode " + znode
				+ (" and watcherSet=" + watcherSet + "; data=")
				+ (data == null ? "null" : data.length == 0 ? "empty" : StringUtils.abbreviate(Bytes.toStringBinary(data), 32))));
	}

	/**
	 * Recursively print the current state of ZK (non-transactional)
	 * 
	 * @param root
	 *            name of the root directory in zk to print
	 * @throws KeeperException
	 */
	public static void logZKTree(ZooKeeperWatcher zkw, String root) throws KeeperException {
		if (!LOG.isDebugEnabled())
			return;
		LOG.debug("Current zk system:");
		String prefix = "|-";
		LOG.debug(prefix + root);
		logZKTree(zkw, root, prefix);
	}

	/**
	 * Helper method to print the current state of the ZK tree.
	 * 
	 * @see #logZKTree(ZooKeeperWatcher, String)
	 * @throws KeeperException
	 *             if an unexpected exception occurs
	 */
	protected static void logZKTree(ZooKeeperWatcher zkw, String root, String prefix) throws KeeperException {
		List<String> children = ZKUtil.listChildrenNoWatch(zkw, root);
		if (children == null)
			return;
		for (String child : children) {
			LOG.debug(prefix + child);
			String node = ZKUtil.joinZNode("/".equals(root) ? "" : root, child);
			logZKTree(zkw, node, prefix + "---");
		}
	}

}