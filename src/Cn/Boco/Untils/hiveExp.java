package Cn.Boco.Untils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.net.URI;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

import Cn.Boco.Manager.ThreadPool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class hiveExp {

	/**
	 * ����sql
	 * 
	 * @param tableName
	 * @param sql
	 * @return
	 */

	public static Logger logger = Logger.getLogger(hiveExp.class);

	public static String insrtSql(String tableName, String sql) {
		return "INSERT INTO TABLE TMP.AA" + tableName + " " + sql;
	}

	/**
	 * ���ݱ���������ƴ�ӳ���ʱ����sql
	 * 
	 * @param columns
	 * @param tableName
	 * @param fileseparator
	 * @return
	 */
	public static String tableCreateSql(int columns, String tableName, String fileseparator) {
		StringBuffer sb = new StringBuffer();
		sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS TMP.AA" + tableName + "(");
		for (int i = 1; i < columns; i++) {
			sb.append("A").append(i).append(" String,");
		}
		sb.append("A").append(columns).append(" String )");
		sb.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + fileseparator + "' LOCATION '/user/boco/impala_tmp/")
				.append(tableName).append("'");
		return sb.toString();
	}

	/**
	 * ���null�ֶ����ó�''
	 * 
	 * @param tableName
	 * @return
	 */
	public static String tableNullSet(String tableName) {
		return "ALTER TABLE TMP.AA" + tableName + " SET TBLPROPERTIES('serialization.null.format' = '')";
	}

	/**
	 * ɾ����ʱ��
	 * 
	 * @param tableName
	 * @return
	 */
	public static Configuration haKerberosConf() 
	{
		Configuration conf = new Configuration();
		String if_ha_flag=ThreadPool.if_ha;
		String node1=ThreadPool.namenode_main;
		String node2=ThreadPool.namenode_bak;
		String if_kerberos_flag=ThreadPool.if_kerberos;
		String kerberos_count=ThreadPool.kerberos_count;
		  if(kerberos_count==null)
		  {
			  kerberos_count="boco/admin@HADOOP.COM";
		  }
		  if(if_ha_flag==null)
		  {
			  if_ha_flag="true";
		  }
		  if(node1==null)
		  {
			  node1="cloud001:8020"; 
		  }
		  if(node2==null)
		  {
			  node2="cloud003:8020"; 
		  }
		  if(if_kerberos_flag==null)
		  {
			  if_kerberos_flag="true"; 
		  }
		if("true".equals(if_ha_flag))
		 {
			 conf.set("dfs.nameservices", "nameservice1");
		     conf.set("dfs.ha.namenodes.nameservice1", "namenode135,namenode111");
		     conf.set("dfs.namenode.rpc-address.nameservice1.namenode135", node1);
		     conf.set("dfs.namenode.rpc-address.nameservice1.namenode111", node2);
		     conf.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		     conf.set("fs.defaultFS", "hdfs://" + "nameservice1" + ":" + "8020"); 
		 }
		 else
		 {
			 //conf.set("dfs.nameservices", "nameservice1");
		     //conf.set("dfs.ha.namenodes.nameservice1", "namenode135,namenode111");
		     //conf.set("dfs.namenode.rpc-address.nameservice1.namenode135", node1);
		     //conf.set("dfs.namenode.rpc-address.nameservice1.namenode111", node1);
		     //conf.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		     conf.set("fs.defaultFS", "hdfs://" + node1);  
		 }
		//String uri = "hdfs://hdfsNameServer:8020";
		//FileSystem fs = FileSystem.get(URI.create(uri), conf);
		//conf.addResource("conf/core-site.xml"); 
        //conf.addResource("conf/hdfs-site.xml");
		
        if("true".equals(if_kerberos_flag))
        {
        try {
        	//conf.set("fs.defaultFS", "hdfs://" + "10.12.1.215" + ":" + "8020");
        	 Util.logger.info("��֤kerberos");
            conf.set("java.security.krb5.conf","conf/krb5.conf");
			conf.set("hadoop.security.authentication", "Kerberos");
			conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@HADOOP.COM");
		     // conf.set("keytab.file","C:/Users/gaoyu/Desktop/BOCO.keytab");
		      //conf.set("keytab.file","/config/BOCO.keytab");
		      UserGroupInformation.setConfiguration(conf);
		      UserGroupInformation.loginUserFromKeytab(kerberos_count, "conf/boco.keytab");      
		     // Connection con = DriverManager.getConnection("jdbc:hive2://10.12.1.217:10000/default;principal=hive/cloud003@HADOOP.COM");
		 } catch (Exception e) {
			  Util.logger.info("��֤kerberos�쳣"+e);
			  return null;
			  }
        }
        return conf;
	}
	public static String dropTable(String tableName) {
		return "DROP TABLE IF EXISTS TMP.AA" + tableName;
	}

	/**
	 * ��hdfs�ϻ�ȡ�ļ�
	 * 
	 * @param remote
	 * @param fileedcoding
	 * @param local
	 * @throws IOException
	 */
	public static Long getFileStream(String remote, String local, String fileedcoding) throws IOException {
		//Configuration conf = new Configuration();
		Configuration conf=haKerberosConf();
        FileSystem fs = FileSystem.get(conf);

		Long records = 0L;

		FileStatus[] fstat = fs.listStatus(new Path(remote));
		Path[] listPath = FileUtil.stat2Paths(fstat);
		if (null == fstat) {
			return 0l;
		}
		Path p1 = null;
		for (FileStatus fsa : fstat) {
			 Util.logger.info("---------"+fsa.getPath().toString());
		}
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(local), fileedcoding));

		for (Path p : listPath) {
			// logger.info(p);
			 logger.info(p.toString());
			if (p.toString().contains("hive-staging_hive")) {
				continue;
			}
			FSDataInputStream in = fs.open(p);
			BufferedReader buff = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			String str = "";
			// Long size = 0L;
			while ((str = buff.readLine()) != null) {
				records++;
				bw.write(str.replaceAll("\\\\N", "").replaceAll("NaN", ""));
				bw.newLine();
			}
			in.close();
		}
		bw.close();
		return records;
	}

	/**
	 * ʵ�ִ�hdfs�ִ�С�ļ��洢�������Ҫѹ����ѹ����ɾ��δѹ���ļ��𣬷����ļ��б�
	 * 
	 * @param remote
	 *            hdfs���ļ�Ŀ¼
	 * @param local
	 *            ���ص����ص��ļ�Ŀ¼+�ļ�ǰ׺
	 * @param fileedcoding
	 *            �ļ�����
	 * @param fileSize
	 *            �ָ��ļ���С
	 * @param compressType
	 *            ѹ����ʽ gz
	 * @return �������ɵ��ļ��б�
	 * @throws IOException
	 */
	public synchronized static List<String> getFilePage(String remote, String local, String fileedcoding,
			Long limitSize, String fileextensions, String tab_head, int if_tab_head) throws IOException {
		List<String> list = new ArrayList<String>();
		Configuration conf=haKerberosConf();
		//String uri = "hdfs://hdfsNameServer:8020";
		//FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		//conf.addResource("conf/core-site.xml"); 
		//conf.addResource("conf/hdfs-site.xml");
		// Long records = 0L;
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fstat = fs.listStatus(new Path(remote));
		Path[] listPath = FileUtil.stat2Paths(fstat);
		// ������һ���ļ�
		int page = 101;
		String localFile = local + "_P" + String.valueOf(page).substring(1, 3) + "." + fileextensions;
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(localFile), fileedcoding));
		// д��ͷ
		if (if_tab_head == 1 && tab_head != null) {
			bw.write(tab_head);
		}
		Long size = 0L;
		for (Path p : listPath) {
			// logger.info(p);
			// logger.info(p.toString());
			if (p.toString().contains("hive-staging_hive")) {
				continue;
			}

			FSDataInputStream in = fs.open(p);
			BufferedReader buff = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			String str = "";

			/*
			 * int q=0; if(if_tab_head==1&&q==0&&tab_head!=null) {
			 * bw.write(tab_head); bw.newLine(); q++; }
			 */

			while ((str = buff.readLine()) != null) {
				size += str.getBytes().length;
				// logger.info(size);

				/*
				 * if(if_tab_head==1&&q==0&&tab_head!=null) {
				 * bw.write(tab_head); bw.newLine(); q++; }
				 */

				bw.write(str.replaceAll("\\\\N", "").replaceAll("NaN", ""));
				bw.newLine();
				// ��ҳ
				if (size >= limitSize) {
					// ѹ�����ӵ�down���б���ȥ
					bw.close();
					// if (if_tab_head == 1 && tab_head != null) {
					// // logger.info("��ͷ "+tab_head+"�ļ�����"+localFile);
					// insertNewLine(localFile, tab_head, 0, fileedcoding);
					// }
					list.add(localFile);
					// ��ʼ��
					page++;
					size = 0L;
					localFile = local + "_P" + String.valueOf(page).substring(1, 3) + "." + fileextensions;
					bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(localFile), fileedcoding));
					if (if_tab_head == 1 && tab_head != null) {
						bw.write(tab_head);
					}
					// q=0;
				}
			}
			in.close();
		}
		bw.close();
		// if (if_tab_head == 1 && tab_head != null) {
		// logger.info("��ͷ " + tab_head + "�ļ�����" + localFile);
		// insertNewLine(localFile, tab_head, 0, fileedcoding);
		// }
		// ���һ���ļ����������.csv�ĳ�_END.csvȻ������б���
		String endFile = localFile.replace("." + fileextensions, "_END." + fileextensions);
		FileUtils.renameFile(localFile, endFile);
		list.add(endFile);
		return list;
	}

	/**
	 * ʵ�ִ�hdfs���ļ��洢�������Ҫѹ����ѹ����ɾ��δѹ���ļ��𣬷����ļ��б�
	 * 
	 * @param remote
	 *            hdfs���ļ�Ŀ¼
	 * @param local
	 *            ���ص����ص��ļ�Ŀ¼+�ļ�ǰ׺
	 * @param fileedcoding
	 *            �ļ�����
	 * 
	 * @param compressType
	 *            ѹ����ʽ gz
	 * @return �������ɵ��ļ��б�
	 * @throws IOException
	 */
	public synchronized static List<String> getFilePage(String remote, String local, String fileedcoding,
			String fileextensions, String tab_head, int if_tab_head) throws IOException {
		List<String> list = new ArrayList<String>();
		Configuration conf=haKerberosConf();
		//String uri = "hdfs://hdfsNameServer/:8020";
		//FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    //conf.addResource("conf/core-site.xml"); 
       // conf.addResource("conf/hdfs-site.xml");
        FileSystem fs = FileSystem.get(conf);
		FileStatus[] fstat = fs.listStatus(new Path(remote));
		Path[] listPath = FileUtil.stat2Paths(fstat);
		// �����ļ�

		String localFile = local + "." + fileextensions;
		logger.info("�����ļ� " + localFile);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(localFile), fileedcoding));
		if (if_tab_head == 1 && tab_head != null) {
			bw.write(tab_head);
		}
		for (Path p : listPath) {
			if (p.toString().contains("hive-staging_hive")) {
				continue;
			}

			FSDataInputStream in = fs.open(p);
			BufferedReader buff = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			String str = "";
			// int i=0;//��ͷֻ��һ��

			while ((str = buff.readLine()) != null) {
				// ��ͷ
				bw.write(str.replaceAll("\\\\N", "").replaceAll("NaN", ""));
				bw.newLine();
			}
			in.close();
		}
		bw.close();
		// if (if_tab_head == 1 && tab_head != null) {
		// logger.info("��ͷ " + tab_head);
		// insertNewLine(localFile, tab_head, 0, fileedcoding);
		// }
		list.add(localFile);
		return list;
	}

	/**
	 * ɾ���ļ���
	 * 
	 * @param fileName
	 * @throws IOException
	 */
	public static void deleteFile(String fileName) throws IOException {
		//Configuration conf = new Configuration();
		//String uri = "hdfs://hdfsNameServer:8020";
		//FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    //conf.addResource("conf/core-site.xml"); 
       // conf.addResource("conf/hdfs-site.xml");
		Configuration conf=haKerberosConf();
        FileSystem fs = FileSystem.get(conf);
		Path f = new Path(fileName);
		boolean isExists = fs.exists(f);
		if (isExists) { // is exists, delete
			boolean isDel = fs.delete(f, true);
			logger.info(fileName + "  delete? \t" + isDel);
		} else {
			logger.info(fileName + "  exist? \t" + isExists);
		}

	}

	public static void insertNewLine(String file_name, String insertContent, int line, String fileedcoding) {

		// �����ļ���һ�У���ͷ
		try {

			File srcFile = new File(file_name);// ���ȴ����ļ�,�ļ�����:1 2 3

			if (srcFile.exists()) {
				String file_name_temp = file_name + "_temp";
				File temp = new File(file_name_temp);

				RandomAccessFile read = new RandomAccessFile(srcFile, "rw");

				RandomAccessFile insert = new RandomAccessFile(temp, "rw");

				String str = "";

				int index = 0;

				while (null != (str = read.readLine())) {
					str = new String(str.getBytes("iso8859-1"), "utf-8");
					if (index == line) {// ����д���к�ʱ

						insert.write((insertContent + str + "\n").getBytes());// д��������+ԭ������

					} else {

						insert.write((str + "\n").getBytes());// д��ԭ������

					}

					index++;

				}

				if (index < line) {// �кŴ����ļ�����,���ļ�ĩλ���������

					long length = temp.length();// ԭ���ļ�����

					insert.seek(length);

					insert.write(insertContent.getBytes());// д���ļ�ĩβ��

				}

				insert.close();

				read.close();
				srcFile.delete();
				FileUtils.renameFile(file_name_temp, file_name);

				/*
				 * BufferedWriter insert_1 = new BufferedWriter(new
				 * OutputStreamWriter(new FileOutputStream(srcFile),
				 * fileedcoding)); BufferedReader read_1 = new
				 * BufferedReader(new InputStreamReader(new
				 * FileInputStream(temp),fileedcoding)); // read = new
				 * RandomAccessFile(srcFile, "rw");
				 * 
				 * 
				 * 
				 * //insert = new RandomAccessFile(temp, "rw");
				 * 
				 * while (null != (str = read_1.readLine())) {//����ʱ�ļ�����д��Դ�ļ�
				 * 
				 * insert_1.write((str)); insert_1.newLine(); }
				 * 
				 * 
				 * 
				 * read_1.close();
				 * 
				 * insert_1.close();
				 * 
				 * temp.delete();//ɾ����ʱ�ļ�
				 */
				logger.info("��ʱ�ļ��������� " + file_name_temp + " Ϊ " + file_name);
				logger.info("--------------End ----------");

			}

		} catch (Exception e) {

			e.printStackTrace();

		}

	}

	public static void main(String[] args) {

		insertNewLine("F:/chaodao/b.csv", "line0d \n", 0, "UTF-8");

		// List<String> lists = getFilePage("/user/boco/test", "E:/testdir/5",
		// "GBK", 100L, "CSV","day_id|hour_id",1);
		// for (String list : lists) {
		// logger.info(list);
		// }
		// getFileStream("/user/boco/impala_tmp/aaaaa", "d:/test/112",
		// "GBK");

	}

}
