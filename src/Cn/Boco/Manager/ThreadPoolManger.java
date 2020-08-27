package Cn.Boco.Manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
//import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Connection;
//import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import Cn.Boco.SonTask.SonThread_Task;
import Cn.Boco.Untils.Util;

public class ThreadPoolManger {
	private static String taskid;
	public static Logger logger = Logger.getLogger(ThreadPoolManger.class);
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("conf/log4j_son.properties");
		/*测试
		args = new String[2];
		args[0] = "1";
		args[1] = "1";*/
		
		String priorityid = args[0];
		String modle = "0";// 0 不管有无配置IP均执行，1只执行没有配置ip的和本机的任务，2 只执行本机器的
		if (args.length > 1) {
			modle = args[1];
		}
		logger.info("命令参数priority:"+priorityid+", modle:"+modle);
		
		ThreadPool.getInstance().start();

		Connection con = null;
		Statement st = null;
		ResultSet rs = null;

		String date = Util.getData();
		// String sql = "select to_char(start_time, 'yyyymmddhh24miss')
		// start_time, d.ftp_ip, d.ftp_port, d.ftp_user, d.ftp_pwd, f.FILE_PATH,
		// a.task_id, a.content_sql, a.order_id, a.CONECTION_STR, e.url wsurl,
		// e.fun_name funcname,a.file_name filename,b.file_edcoding
		// fileedcoding,b.if_tab_head if_tab_head,b.file_separator
		// fileseparator,b.file_extensions
		// fileextensions,b.database_type,b.upload_type,b.compress_type,b.file_size,a.encrypt_key
		// from sys_sharedata_task a left join SYS_SHAREDATA_ORDER_NEW b on
		// a.order_id = b.order_id left join SYS_SHAREDATA_FTP d on b.ftp_id =
		// d.ftp_id left join sys_sharedata_notifysrv e on e.order_id =
		// a.order_id left join sys_sharedata_ftp_path f on b.ftp_id=f.ftp_id
		// and b.path_id=f.path_id left join sys_sharedata_relation_so g on
		// a.order_id=g.order_id left join sys_sharedata_server_config h on
		// g.server_id=h.server_id where a.state = '未执行' and a.start_time <
		// to_date('"
		// + date + "','YYYYMMDDHH24MISS') and a.priority";
		String sql = "select to_char(start_time, 'yyyymmddhh24miss') start_time,  d.ftp_ip,  d.ftp_port,  d.ftp_user,  d.ftp_pwd,  f.FILE_PATH,  a.task_id,  a.content_sql,  a.order_id,  a.CONECTION_STR,  e.url wsurl,  e.fun_name funcname,a.file_name filename,b.file_edcoding fileedcoding,b.if_tab_head if_tab_head,b.file_separator fileseparator,b.file_extensions fileextensions,b.database_type,b.upload_type,b.compress_type,b.file_size,a.encrypt_key  from sys_sharedata_task a  left join SYS_SHAREDATA_ORDER_NEW b  on a.order_id = b.order_id  left join SYS_SHAREDATA_FTP d  on b.ftp_id = d.ftp_id  left join sys_sharedata_notifysrv e  on e.order_id = a.order_id  left join sys_sharedata_ftp_path f  on b.ftp_id=f.ftp_id  and b.path_id=f.path_id  left join sys_sharedata_relation_so g on a.order_id=g.order_id left join sys_sharedata_server_config h on g.server_id=h.server_id where a.state = '未执行'  and a.start_time < sysdate and  a.priority";
		String upsql = "";
		if ("1".equals(priorityid)) {
			sql = sql + "=1";
		} else if ("2".equals(priorityid)) {
			sql = sql + "=2";
		} else if ("3".equals(priorityid)) {
			sql = sql + "=3";
		} else if ("4".equals(priorityid)) {
			sql = sql + ">3";
		}

		InetAddress netAddress = null;
		String localIP = "未知";
		try {
			netAddress = InetAddress.getLocalHost();
			localIP = netAddress.getHostAddress();
		} catch (UnknownHostException e1) {
			logger.error("获取本机IP地址失败！");
		}

		if ("1".equals(modle.trim())) {
			sql = sql + " and (h.server_ip is null or h.server_ip='" + localIP + "')";
		} else if ("2".equals(modle.trim())) {
			sql = sql + " and h.server_ip='" + localIP + "'";
		}
		logger.info("调度表查询语句:" + sql);
		do {
			try {
				con = Util.getConnection(ThreadPool.URL, ThreadPool.USER, ThreadPool.PASSWD);
				while (con == null) {
					try {
						Thread.sleep(60000l);
						con = Util.getConnection(ThreadPool.URL, ThreadPool.USER, ThreadPool.PASSWD);
					} catch (InterruptedException e) {
						// 不做处理
					}

				}
				st = con.createStatement();
				rs = st.executeQuery(sql);
				long item_count = 0L;
				long task_count = 0L;
				
				while (rs.next()) {
					item_count ++;
					try {
						String sqlmetainfo = rs.getString("conection_str");// 获取数据源
						Blob bl = rs.getBlob("content_sql");// sql语句
						BufferedReader bf = new BufferedReader(new InputStreamReader(bl.getBinaryStream(),ThreadPool.DbEncouding));
						String temp = "";
						StringBuffer strsql = new StringBuffer();
						while ((temp = bf.readLine()) != null) {
							strsql.append(temp);
						}

						// taskid = rs.getBigDecimal("task_id").toBigInteger();
						taskid = rs.getString("task_id");
						upsql = "update sys_sharedata_task set state='执行中'  where task_id='" + taskid + "'";
						// String servicedesc = rs.getString("service_desc");
						String url = rs.getString("ftp_ip");
						Integer port = rs.getInt("ftp_port");
						if (port == 0) {
							port = 21;
						}
						String username = rs.getString("ftp_user");
						String password = rs.getString("ftp_pwd");
						String remotePath = rs.getString("file_path");
						String wsurl = rs.getString("wsurl");
						String funcname = rs.getString("funcname");
						// 新增文件名，后缀，编码从数据库读出
						String fileName = rs.getString("filename");
						String databaseType = rs.getString("database_type");
						if (StringUtils.isBlank(databaseType)) {
							databaseType = "非HIVE";
						}
						String uploadType = rs.getString("upload_type");
						if (StringUtils.isBlank(uploadType)) {
							uploadType = "0";// 按照默认方式进行上报
						}
						String compressType = rs.getString("compress_type");
						if (StringUtils.isBlank(compressType)) {
							compressType = "0";// 默认不压缩
						}
						String fileSize = rs.getString("file_size");
						if (StringUtils.isBlank(fileSize)) {
							fileSize = "";// 默认不分割
						}
						String encryptKey = rs.getString("encrypt_key");
						if (StringUtils.isBlank(encryptKey)) {
							encryptKey = "";// 默认不加密
						}

						String fileedcoding = rs.getString("fileedcoding");
						// 是否有表头
						int if_tab_head = rs.getInt("if_tab_head");

						if (fileedcoding == null || "".equals(fileedcoding))
							fileedcoding = "utf-8";
						String fileseparator = rs.getString("fileseparator");
						if (fileseparator == null || "".equals(fileseparator))
							fileseparator = "|";
						String fileextensions = rs.getString("fileextensions");
						if (fileextensions == null || "".equals(fileextensions))
							fileextensions = "csv";
						String start_time = rs.getString("start_time");
						// System.err.println("修改参数：" + fileedcoding + " " +
						// fileseparator + " " + fileextensions);
						// String namespace = "http://tempuri.org/";
						String namespace = "http://dss.boco/UploadNotifyToTS";
						String orderid = rs.getString("order_id");
						if (sqlmetainfo != null && !"".equals(sqlmetainfo) && strsql != null && !"".equals(strsql)) {
							SonThread_Task sttask = new SonThread_Task();
							sttask.setSql(strsql.toString());
							sttask.setSqlmetainfo(sqlmetainfo);
							// sttask.setServicedesc(servicedesc);
							sttask.setTaskid(taskid);
							sttask.setUrl(url);
							sttask.setPort(port);
							sttask.setUsername(username);
							sttask.setPassword(password);

							if (remotePath == null || "null".equals(remotePath)) {
								remotePath = "";
							}
							// remotePath = "/" + remotePath + "/";
							// while (remotePath.indexOf("//") > -1) {
							// remotePath = remotePath.replace("//", "/");
							// }
							sttask.setRemotePath(remotePath);

							sttask.setWSurl(wsurl);
							sttask.setFuncname(funcname);

							sttask.setFileName(fileName);
							sttask.setFileseparator(fileseparator);
							sttask.setFileedcoding(fileedcoding);
							sttask.setFileextensions(fileextensions);

							sttask.setStart_time(start_time);
							sttask.setOrderid(orderid);
							sttask.setNamespace(namespace);
							sttask.setDatabaseType(databaseType);
							sttask.setUploadType(uploadType);
							sttask.setCompressType(compressType);
							sttask.setFileSize(fileSize);
							sttask.setEncryptKey(encryptKey);
							sttask.setIf_tab_head(if_tab_head);
							Util.Start(taskid);
							ThreadPool.getInstance().addTask(sttask);
							
							logger.info("添加任务，文件名前缀:"+fileName);
							task_count ++;
							
						} else {
							Util.Finsh(taskid, "SQL语句是空的 或者conection_str是空");
							Util.WebService(wsurl, namespace, funcname, orderid, 2 + "", "无SQL语句或者无数据源信息 或者其他例外", url,
									port + "", username, password, "utf-8", 2 + "", "NULL", remotePath, date, "|");
						}

					} catch (SQLException e) {
						logger.warn("在获取数据库任务信息时候 发生SQL异常 此异常在ThreadPoolManger中发生 链接字符串 " + ThreadPool.URL + ","
								+ ThreadPool.USER + "," + ThreadPool.PASSWD + " SQL语句为 : " + sql + "更新SQL语句:" + upsql,
								e);
						Util.Finsh(taskid, "在获取数据库任务信息时候 发生SQL异常 此异常在ThreadPoolManger中发生 链接字符串 " + ThreadPool.URL + ","
								+ ThreadPool.USER + "," + ThreadPool.PASSWD + " SQL语句为 : " + sql + "更新SQL语句:" + upsql + e.getMessage());

					}
				}
				logger.info("本次执行完毕,查询记录数："+item_count+", 添加任务数："+task_count);

				con.commit();
				Util.release(rs, st, con);
			} catch (SQLException e) {
				Util.release(rs, st, con);
				logger.warn("在获取数据库任务信息时候 发生SQL异常 此异常在ThreadPoolManger中发生 链接字符串" + ThreadPool.URL + ","
						+ ThreadPool.USER + "," + ThreadPool.PASSWD + " SQL语句为 : " + sql + "更新SQL语句" + upsql, e);
				Util.Finsh(taskid, "在获取数据库任务信息时候 发生SQL异常 此异常在ThreadPoolManger中发生 链接字符串" + ThreadPool.URL + ","
						+ ThreadPool.USER + "," + ThreadPool.PASSWD + " SQL语句为 : " + sql + "更新SQL语句" + upsql + e.getMessage());
			} catch (IOException e) {
				Util.release(rs, st, con);
				logger.warn("读取SQL的BLOB字段时候发生IO异常  SQL语句为  " + sql + "异常", e);
				Util.Finsh(taskid, "读取SQL的BLOB字段时候发生IO异常  SQL语句为  " + sql + e.getMessage());
			} catch (NullPointerException e) {
				Util.release(rs, st, con);
				logger.warn("空指针", e);
				Util.Finsh(taskid, "程序内部出错，遇到空指针 :"+e.getMessage());
			}
			try {
				Thread.sleep(60000l);
			} catch (InterruptedException e) {
				// 不做处理
			}
			upsql = "";
		} while (true);

	}
}
