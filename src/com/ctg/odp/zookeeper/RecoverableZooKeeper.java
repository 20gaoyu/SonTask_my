package com.ctg.odp.zookeeper;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.SetDataRequest;

/**
 * A zookeeper that can handle 'recoverable' errors. To handle recoverable errors, developers need to realize that there are two classes of requests: idempotent and non-idempotent requests. Read requests and unconditional sets and deletes are examples of idempotent requests, they can be reissued with the same results. (Although, the delete may throw a NoNodeException on reissue
 * its effect on the ZooKeeper state is the same.) Non-idempotent requests need special handling, application and library writers need to keep in mind that they may need to encode information in the data or name of znodes to detect retries. A simple example is a create that uses a sequence flag. If a process issues a create("/x-", ..., SEQUENCE) and gets a connection loss
 * exception, that process will reissue another create("/x-", ..., SEQUENCE) and get back x-111. When the process does a getChildren("/"), it sees x-1,x-30,x-109,x-110,x-111, now it could be that x-109 was the result of the previous create, so the process actually owns both x-109 and x-111. An easy way around this is to use "x-process id-" when doing the create. If the process
 * is using an id of 352, before reissuing the create it will do a getChildren("/") and see "x-222-1", "x-542-30", "x-352-109", x-333-110 ". The process will know that the original create succeeded an the znode it created is " x-352-109".
 */
public class RecoverableZooKeeper {
    private static final Log LOG = LogFactory.getLog(RecoverableZooKeeper.class);
    // the actual ZooKeeper client instance
    private volatile ZooKeeper zk;
    private final RetryCounterFactory retryCounterFactory;
    // An identifier of this process in the cluster
    private final String identifier;
    private final byte[] id;

    // The metadata attached to each piece of data has the
    // format:
    // <magic> 1-byte constant
    // <id length> 4-byte big-endian integer (length of next field)
    // <id> identifier corresponding uniquely to this process
    // It is prepended to the data supplied by the user.

    // the magic number is to be backward compatible
    private static final byte MAGIC = (byte) 0XFF;
    private static final int MAGIC_SIZE = Bytes.SIZEOF_BYTE;
    private static final int ID_LENGTH_OFFSET = MAGIC_SIZE;
    private static final int ID_LENGTH_SIZE = Bytes.SIZEOF_INT;

    public RecoverableZooKeeper(String quorumServers, int sessionTimeout, Watcher watcher, int maxRetries, int retryIntervalMillis) throws IOException {
        this.zk = new ZooKeeper(quorumServers, sessionTimeout, watcher);
        this.retryCounterFactory = new RetryCounterFactory(maxRetries, retryIntervalMillis);

        // the identifier = processID@hostName
        this.identifier = ManagementFactory.getRuntimeMXBean().getName();
        LOG.info("The identifier of this process is " + identifier);
        this.id = Bytes.toBytes(identifier);
    }

    /**
     * delete is an idempotent operation. Retry before throwing exception. This function will not throw NoNodeException if the path does not exist.
     */
    public void delete(String path, int version) throws InterruptedException, KeeperException {
        RetryCounter retryCounter = retryCounterFactory.create();
        while (true) {
            try {
                zk.delete(path, version);
                return;
            } catch (KeeperException e) {
                if (e.code() == Code.NONODE) {
                    LOG.info("Node " + path + " already deleted. Assuming that a previous attempt succeeded.");
                    return;
                }else{
                    retryOrThrow(retryCounter, e, "delete");
                }
            }
            retryCounter.sleepUntilNextRetry();
            retryCounter.useRetry();
        }
    }

    /**
     * exists is an idempotent operation. Retry before throwing exception
     * 
     * @return A Stat instance
     */
    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        RetryCounter retryCounter = retryCounterFactory.create();
        while (true) {
            try {
                return zk.exists(path, watcher);
            } catch (KeeperException e) {
                retryOrThrow(retryCounter, e, "exists");
            }
            retryCounter.sleepUntilNextRetry();
            retryCounter.useRetry();
        }
    }

    /**
     * exists is an idempotent operation. Retry before throwing exception
     * 
     * @return A Stat instance
     */
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        RetryCounter retryCounter = retryCounterFactory.create();
        while (true) {
            try {
                return zk.exists(path, watch);
            } catch (KeeperException e) {
                retryOrThrow(retryCounter, e, "exists");
            }
            retryCounter.sleepUntilNextRetry();
            retryCounter.useRetry();
        }
    }

    private void retryOrThrow(RetryCounter retryCounter, KeeperException e, String opName) throws KeeperException {
        LOG.warn("Possibly transient ZooKeeper exception: " + e);
        String format = "ZooKeeper %s failed after %d retries";
        switch (e.code()) {
        case CONNECTIONLOSS:
        case SESSIONEXPIRED:
        case OPERATIONTIMEOUT:
            if (!retryCounter.shouldRetry()) {
                LOG.error(String.format(format, opName , retryCounter.getMaxAttempts()));
                throw e;
            }
            break;
        default:
            throw e;
        }
    }

    /**
     * getChildren is an idempotent operation. Retry before throwing exception
     * 
     * @return List of children znodes
     * @throws KeeperException
     *             ,InterruptedException
     */
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        RetryCounter retryCounter = retryCounterFactory.create();
        while (true) {
            try {
                return zk.getChildren(path, watcher);
            } catch (KeeperException e) {
                retryOrThrow(retryCounter, e, "getChildren");
            }
            retryCounter.sleepUntilNextRetry();
            retryCounter.useRetry();
        }
    }

    /**
     * getChildren is an idempotent operation. Retry before throwing exception
     * 
     * @return List of children znodes
     */
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        RetryCounter retryCounter = retryCounterFactory.create();
        while (true) {
            try {
                return zk.getChildren(path, watch);
            } catch (KeeperException e) {
                retryOrThrow(retryCounter, e, "getChildren");
            }
            retryCounter.sleepUntilNextRetry();
            retryCounter.useRetry();
        }
    }

    /**
     * getData is an idempotent operation. Retry before throwing exception
     * 
     * @return Data
     */
    public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        RetryCounter retryCounter = retryCounterFactory.create();
        while (true) {
            try {
                byte[] revData = zk.getData(path, watcher, stat);
                return this.removeMetaData(revData);
            } catch (KeeperException e) {
                retryOrThrow(retryCounter, e, "getData");
            }
            retryCounter.sleepUntilNextRetry();
            retryCounter.useRetry();
        }
    }

    /**
     * getData is an idemnpotent operation. Retry before throwing exception
     * 
     * @return Data
     */
    public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        RetryCounter retryCounter = retryCounterFactory.create();
        while (true) {
            try {
                byte[] revData = zk.getData(path, watch, stat);
                return this.removeMetaData(revData);
            } catch (KeeperException e) {
                retryOrThrow(retryCounter, e, "getData");
            }
            retryCounter.sleepUntilNextRetry();
            retryCounter.useRetry();
        }
    }

    /**
     * setData is NOT an idempotent operation. Retry may cause BadVersion Exception Adding an identifier field into the data to check whether badversion is caused by the result of previous correctly setData
     * 
     * @return Stat instance
     */
    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        RetryCounter retryCounter = retryCounterFactory.create();
        while (true) {
            byte[] newData = appendMetaData(data);
            try {
                return zk.setData(path, newData, version);
            } catch (KeeperException e) {
                if (e.code() == Code.BADVERSION) {
                    // try to verify whether the previous setDatasuccess or not
                    Stat stat = new Stat();
                    byte[] revData = zk.getData(path, false, new Stat());
                    // the bad version is caused by previous successful setData
                    if (Bytes.equals(revData, newData)) {
                        return stat;
                    }
                } else {
                    // throw other exceptions and verified bad version exceptions
                    retryOrThrow(retryCounter, e, "setData");
                }
            }
            retryCounter.sleepUntilNextRetry();
            retryCounter.useRetry();
        }
    }

    /**
     * <p>
     * NONSEQUENTIAL create is idempotent operation. Retry before throwing exceptions. But this function will not throw the NodeExist exception back to the application.
     * </p>
     * <p>
     * But SEQUENTIAL is NOT idempotent operation. It is necessary to add identifier to the path to verify, whether the previous one is successful or not.
     * </p>
     * 
     * @return Path
     */
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
        byte[] newData = appendMetaData(data);
        switch (createMode) {
        case EPHEMERAL:
        case PERSISTENT:
            return createNonSequential(path, newData, acl, createMode);

        case EPHEMERAL_SEQUENTIAL:
        case PERSISTENT_SEQUENTIAL:
            return createSequential(path, newData, acl, createMode);

        default:
            throw new IllegalArgumentException("Unrecognized CreateMode: " + createMode);
        }
    }

    private String createNonSequential(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
        RetryCounter retryCounter = retryCounterFactory.create();
        boolean first = true; // False for first attempt, true for all
                              // retries.
        while (true) {
            try {
                return zk.create(path, data, acl, createMode);
            } catch (KeeperException e) {
                if (e.code() == Code.NODEEXISTS) {
                    if (first) {
                        LOG.info("Node " + path + " already exists and this is not a " + "retry");
                        throw e;
                    }
                    byte[] currentData = zk.getData(path, false, null);
                    if (currentData == null || Bytes.compareTo(currentData, data) != 0) {
                        LOG.error("Node " + path + " already exists with " + Bytes.toStringBinary(currentData) + ", could not write " + Bytes.toStringBinary(data));
                        throw e;
                    }
                    return path;// We successfully created a non-sequential node
                } else {
                    retryOrThrow(retryCounter, e, "create");
                }
            }
            retryCounter.sleepUntilNextRetry();
            retryCounter.useRetry();
            first = false;
        }
    }

    private String createSequential(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
        RetryCounter retryCounter = retryCounterFactory.create();
        boolean first = true;
        String newPath = path + this.identifier;
        while (true) {
            try {
                if (first) {
                    first = false;
                    return zk.create(newPath, data, acl, createMode);
                }

                String previousResult = findPreviousSequentialNode(newPath);
                if (previousResult != null) {
                    return previousResult;
                }
            } catch (KeeperException e) {
                retryOrThrow(retryCounter, e, "create");
            }
            retryCounter.sleepUntilNextRetry();
            retryCounter.useRetry();
        }
    }

    /**
     * Convert Iterable of {@link ZKOp} we got into the ZooKeeper.Op instances to actually pass to multi (need to do this in order to appendMetaData).
     */
    private Iterable<Op> prepareZKMulti(Iterable<Op> ops)  {
        if (ops == null)
            return null;

        List<Op> preparedOps = new LinkedList<Op>();
        for (Op op : ops) {
            if (op.getType() == ZooDefs.OpCode.create) {
                CreateRequest create = (CreateRequest) op.toRequestRecord();
                preparedOps.add(Op.create(create.getPath(), appendMetaData(create.getData()), create.getAcl(), create.getFlags()));
            } else if (op.getType() == ZooDefs.OpCode.delete) {
                // no need to appendMetaData for delete
                preparedOps.add(op);
            } else if (op.getType() == ZooDefs.OpCode.setData) {
                SetDataRequest setData = (SetDataRequest) op.toRequestRecord();
                preparedOps.add(Op.setData(setData.getPath(), appendMetaData(setData.getData()), setData.getVersion()));
            } else {
                throw new UnsupportedOperationException("Unexpected ZKOp type: " + op.getClass().getName());
            }
        }
        return preparedOps;
    }

    /**
     * Run multiple operations in a transactional manner. Retry before throwing exception
     */
    public List<OpResult> multi(Iterable<Op> ops) throws KeeperException, InterruptedException {
        RetryCounter retryCounter = retryCounterFactory.create();
        Iterable<Op> multiOps = prepareZKMulti(ops);
        while (true) {
            try {
                return zk.multi(multiOps);
            } catch (KeeperException e) {
                retryOrThrow(retryCounter, e, "multi");
            }
            retryCounter.sleepUntilNextRetry();
            retryCounter.useRetry();
        }
    }

    private String findPreviousSequentialNode(String path) throws KeeperException, InterruptedException {
        int lastSlashIdx = path.lastIndexOf('/');
        assert lastSlashIdx != -1;
        String parent = path.substring(0, lastSlashIdx);
        String nodePrefix = path.substring(lastSlashIdx + 1);

        List<String> nodes = zk.getChildren(parent, false);
        List<String> matching = filterByPrefix(nodes, nodePrefix);
        for (String node : matching) {
            String nodePath = parent + "/" + node;
            Stat stat = zk.exists(nodePath, false);
            if (stat != null) {
                return nodePath;
            }
        }
        return null;
    }

    public byte[] removeMetaData(byte[] data) {
        if (data == null || data.length == 0) {
            return data;
        }
        // check the magic data; to be backward compatible
        byte magic = data[0];
        if (magic != MAGIC) {
            return data;
        }

        int idLength = Bytes.toInt(data, ID_LENGTH_OFFSET);
        int dataLength = data.length - MAGIC_SIZE - ID_LENGTH_SIZE - idLength;
        int dataOffset = MAGIC_SIZE + ID_LENGTH_SIZE + idLength;

        byte[] newData = new byte[dataLength];
        System.arraycopy(data, dataOffset, newData, 0, dataLength);

        return newData;

    }

    private byte[] appendMetaData(byte[] data) {
        if (data == null || data.length == 0) {
            return data;
        }

        byte[] newData = new byte[MAGIC_SIZE + ID_LENGTH_SIZE + id.length + data.length];
        int pos = 0;
        pos = Bytes.putByte(newData, pos, MAGIC);
        pos = Bytes.putInt(newData, pos, id.length);
        pos = Bytes.putBytes(newData, pos, id, 0, id.length);
        pos = Bytes.putBytes(newData, pos, data, 0, data.length);

        return newData;
    }

    public long getSessionId() {
        return zk.getSessionId();
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    public States getState() {
        return zk.getState();
    }

    public ZooKeeper getZooKeeper() {
        return zk;
    }

    public byte[] getSessionPasswd() {
        return zk.getSessionPasswd();
    }

    /**
     * Filters the given node list by the given prefixes. This method is all-inclusive--if any element in the node list starts with any of the given prefixes, then it is included in the result.
     *
     * @param nodes
     *            the nodes to filter
     * @param prefixes
     *            the prefixes to include in the result
     * @return list of every element that starts with one of the prefixes
     */
    private static List<String> filterByPrefix(List<String> nodes, String... prefixes) {
        List<String> lockChildren = new ArrayList<String>();
        for (String child : nodes) {
            for (String prefix : prefixes) {
                if (child.startsWith(prefix)) {
                    lockChildren.add(child);
                    break;
                }
            }
        }
        return lockChildren;
    }
}
