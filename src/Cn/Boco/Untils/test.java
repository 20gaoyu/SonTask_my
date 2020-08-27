package Cn.Boco.Untils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.thrift.TException;

import com.ctg.odp.zookeeper.ZooKeeperWatcher;

public class test {
	private static Configuration conf = null;
	private static HiveConf hiveConf = null;
	private static ZooKeeperWatcher zookeeper = null;
	private static int pid = -1;
    private static String hostname = "";
    private static String pidAtHostname = "";
    private static String canonicalHostName = "";
    private static Connection con=null;
    private static ResultSet rs = null;
    private static Statement stmt= null;
	public static void main(String[] args) {
		 //TODO Auto-generated method stub
        /*String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        pid = Integer.parseInt(processName.split("@")[0]);
        // 初始化HOSTNAME
        try {
            hostname = (InetAddress.getLocalHost()).getHostName();
            canonicalHostName = (InetAddress.getLocalHost()).getCanonicalHostName();
        } catch (UnknownHostException e) {
        	 System.out.println("get hostname failed:" + e.getMessage());
            hostname = "unknown";
        }
        pidAtHostname = pid + "@" + hostname;
        System.out.println("--------pid@hostname=" + pidAtHostname);
        conf = new Configuration();
        try {
            hiveConf = HCatUtil.getHiveConf(conf);
            hiveConf.set("hive.metastore.token.signature","DelegationTokenForHiveMetaStoreServer");
            
            System.out.println("******hiveclientcache****:" + hiveConf.getBoolean(HCatConstants.HCAT_HIVE_CLIENT_DISABLE_CACHE, false));
        } catch (IOException e) {
            throw new RuntimeException("init hiveConf failed:" + e.getMessage(), e);
        }
        try {
            ZooKeeperWatcher zookeeper = new ZooKeeperWatcher(conf, pidAtHostname, null, true);
            setZooKeeperWatcher(zookeeper);
        } catch (IOException e) {
            throw new RuntimeException("Connect to zookeeper failed:" + e.getMessage(), e);
        }
        HiveMetaStoreClient hiveClient = null;
        //String partition = "part=" + node + "/yyyymmdd=" + time;
        List<String> tablePartitionNames = null;
        try {
            hiveClient = HCatUtil.getHiveClient(hiveConf);
            tablePartitionNames = hiveClient.listPartitionNames("staging", "system_user_role", Short.MAX_VALUE);
            for(String partition:tablePartitionNames)
            {
            	System.out.println("--------partition:-------"+partition);
            }
        } catch (IOException | TException e) {
            e.printStackTrace();
        } finally {
            hiveClient.close();
        }*/
        String table="system_user_role";
		if(args.length>0)
		{
			table=args[0];
		}

		String driver="org.apache.hive.jdbc.HiveDriver";
		//String url="jdbc:hive2://10.12.1.31:10000/default;principal=hive/cloud31@HADOOP.COM";
		String url="jdbc:hive2://192.168.25.141:10000/merge";
		//String url="jdbc:hive2://10.12.1.31:21050/;auth=noSasl;KrbHostFQDN=cloud31;KrbServiceName=impala";
		String username="hive";
		String password="hive";
		con=Util.getConnection(driver, url, username, password);
		if(con!=null)
		{
			System.out.println("ok");
	        //String sql="select count(*) from merge.system_user_role";
			String sql="show create table "+table;
	        System.out.println(sql);
			
			try {
				stmt = con.createStatement();
				new GetLogThread().start();
				stmt.execute("use merge");
		        rs = stmt.executeQuery(sql);
		        while (rs.next()) {
		        //System.out.println(rs.getInt(1));	
		        	System.out.println(rs.getString(1));
		        }
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        // System.out.println("add file " + addFile);

	        //boolean result = stmt.execute(sql);

		}
		
//		try {
//			System.out.println(hiveExp.getFileStream("/user/boco/GDIPcache/DIM_NE_TAECI","F:/data/data/http/http33.txt","UTF-8"));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
//		String ss="driver:=org.apache.hive.jdbc.HiveDriver#url:=jdbc:hive2://10.230.86.16:10000/default;principal=hive/cloud16@HADOOP.COM#user:=datacenter_v3#password:=";
//		String[] sts = ss.split("#");
//		String driver = sts[0].trim().substring(8);
//		String url = sts[1].substring(5);
//		String username = sts[2].substring(6);
//		String password = sts[3].substring(10);
//		System.out.println(driver+","+url+","+username+","+password);
//		String tts="jdbc:hive2://10.12.1.217:10000/default;principal=hive/cloud003@HADOOP.COM";
//		if(tts.contains("principal"))
//		{
//			System.out.println("ok");
//	}
	}
    public static ZooKeeperWatcher getZooKeeperWatcher() {
        return zookeeper;
    }

    public static void setZooKeeperWatcher(ZooKeeperWatcher zk) {
        zookeeper = zk;
    }
    static class GetLogThread extends Thread {

        public void run() { //真生的输出运行进度的thread
            if (stmt == null) {
                return;
            }
            HiveStatement hiveStatement = (HiveStatement) stmt;
            try {
                while (!hiveStatement.isClosed() && ((HiveStatement) stmt).hasMoreLogs()) {
                    try {
                        for (String log : ((HiveStatement) stmt).getQueryLog(true, 100)) {
                            System.out.println(log);
                        }
                        Thread.currentThread().sleep(500L);
                    } catch (SQLException e) { //防止while里面报错，导致一直退不出循环
                        e.printStackTrace();
                        return;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return;
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
