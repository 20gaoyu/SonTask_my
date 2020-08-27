package com.ctg.odp.zookeeper;

public class OdpConstants {
    /** The delay when re-trying a socket operation in a loop (HBASE-4712) */
    public static final int SOCKET_RETRY_WAIT_MS = 200;

    /** Default client port that the zookeeper listens on */
    public static final String DEFAULT_ZOOKEPER_QUORUMSERVERS = "192.168.25.141";

    /** Default client port that the zookeeper listens on */
    public static final int DEFAULT_ZOOKEPER_CLIENT_PORT = 2181;

    /** Default wait time for the recoverable zookeeper */
    public static final int DEFAULT_ZOOKEEPER_RECOVERABLE_INTERVAL = 10000;

    /** Default wait time for the recoverable zookeeper */
    public static final int DEFAULT_ZOOKEEPER_RECOVERABLE_RETRIES = 3;

    /** Default root path of zookeeper */
    public static final String DEFAULT_ZOOKEEPER_ZNODE_ROOT = "/ts";

    /** Default value for ZooKeeper session timeout */
    public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 60 * 1000;

    public static final String ODP_ZOOKEEPER_SESSION_SESSIONTIMEOUT = "opd.zookeeper.sessionTimeout";

    public static final String ODP_HIVE_QUERY_PID = "hive.query.pid";
    public static final String ODP_HQLSESSION_PERUSER_SIZE = "odp.hqlsession.peruser.size";
    public static final int ODP_DEFAULT_HQLSESSION_PERUSER_SIZE = 2;
    public static final int ODP_HQLSESSION_CHECK_TIME = 30000;

    public static final int REDIS_EXPIRETIME = 3600 * 24 * 7;

    public static final String ODP_ZOOKEEPER_RECOVERY_RETRIES = "odp.zookeeper.recovery.retries";

    public static final String ODP_ZOOKEEPER_RECOVERY_INTERVAL = "odp.zookeeper.recovery.interval";

    public static final String ODP_ZOOKEEPER_QUORUMSERVERS = "odp.zookeeper.quorumServers";

    public static final String ODP_ZOOKEEPER_ZNODE_ROOT = "odp.zookeeper.znode.root";

    public static final String ODP_USERS_KEYTAB_PRINCIPAL_SUFFIX = "odp.kerberos.principal_suffix";

    public static final String ODP_USERS_KEYTAB_DEFAULT_PRINCIPAL_SUFFIX = "/_HOST@ECLOUD.COM";

    public static final String ODP_USERS_KEYTAB_DEFAULT_DIR = "/tmp/keytabs/";

    public static final String ODP_USERS_KEYTAB_FILE_DIR = "odp.users.keytabs.dir";

    public static final String OPD_USERS_KEYTAB_FILENAME_SUFFIX = "odp.users.keytabs.filename";

    public static final String OPD_USERS_KEYTAB_DEFAULT_FILENAME_SUFFIX = ".keytab";

    public static final String OPD_USERS_KEYTAB_TYPE_DEFAULT_SUFFIX = ".app";

    public static final String ODP_HQL_RESULT_ROOT_DIR = "odp.hql.result.root_dir";

    public static final String ODP_HQL_RESULT_DEFAULT_ROOT_DIR = "/apps/odp/hql_result/";

    public static final String ODP_SUPER_USER = "odp.super.user";

    public static final String ODP_DEFAULT_SUPER_USER = "odp";

    public static final String ODP_QUERYID_PREFIX = "pid_";

    public static final String ODP_HQL_RESULT_FILE_SCHEMA = "odp.hql.result.schema";// 结果文件首行添加字段�?

    public static final boolean ODP_DEFAULT_HQL_RESULT_FILE_SCHEMA = true;

    public static final String ODP_SCHEDULE_TASK_ID = "SCHEDULE_TASK_ID";
    public static final String ODP_SCHEDULE_APP_ID = "SCHEDULE_APP_ID";
    public static final String ODP_SCHEDULE_APP_TYPE = "SCHEDULE_APP_TYPE";
    public static final String ODP_SCHEDULE_TASK_INSTANCE_ID = "ODP_SCHEDULE_TASK_INSTANCE_ID";
    public static final String ODP_SCHEDULE_TASK_CRON_LAYNUMBER = "ODP_SCHEDULE_TASK_CRON_LAYNUMBER";
    public static final String ODP_SCHEDULE_MANUAL_EXECUTE = "ODP_SCHEDULE_MANUAL_EXECUTE";

    public static final String KTY_TAB_RELOGIN_DELAY = "user.keytab.relogin.delay";
    public static final long DEFAULT_KEY_TAB_RELOGIN_DELAY = 82800000; // 23小时
    public static final String KTY_TAB_RELOGIN_PERIOD = "user.keytab.relogin.preiod";
    public static final long DEFAULT_KEY_TAB_RELOGIN_PERIOD = 82800000; // 23小时

    public static final String ODP_TENANT_NAME = "TENANT";
    public static final String ODP_MR_JSON_PATH = "JSON";
    public static final String ODP_MR_JSON_EXEC_ENGINE = "EXEC_ENGINE";
    public static final String ODP_TENANT_PATH_DEFAULT_PROFIX = "/apps";
    public static final String ODP_TENANT_KEYTABS_PATH_DEFAULT = "keytabs";
    public static final String ODP_TENANT_DEFAULT_HQL_LIB_PATH = "hql_lib";
    public static final String ODP_TENANT_HQL_LIB_PATH = "odp.tenant.hql.lib.path";
    public static final String ODP_TENANT_DEFAULT_MRJOSN_LIB_PATH = "mrjson_lib";
    public static final String ODP_TENANT_PATH_PROFIX = "odp.tenant.path.profix";
    public static final String ODP_TENANT_MRJSON_LIB_PATH = "odp.tenant.mrjson.lib.path";
    public static final String ODP_MR_JSON_FRAMEWORK_LIB_DEFAULT = "/common/odp/mrjson_framework_lib";
    public static final String ODP_MR_JSON_FRAMEWORK_LIB = "odp.mrjson.framework.lib";
    
    public static final String DELEGATION_TOKEN_FOR_HIVE = "DelegationTokenForHiveMetaStoreServer";

    private OdpConstants() {

    }
}