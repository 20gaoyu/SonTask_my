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
		/*����
		args = new String[2];
		args[0] = "1";
		args[1] = "1";*/
		
		String priorityid = args[0];
		String modle = "0";// 0 ������������IP��ִ�У�1ִֻ��û������ip�ĺͱ���������2 ִֻ�б�������
		if (args.length > 1) {
			modle = args[1];
		}
		logger.info("�������priority:"+priorityid+", modle:"+modle);
		
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
		// g.server_id=h.server_id where a.state = 'δִ��' and a.start_time <
		// to_date('"
		// + date + "','YYYYMMDDHH24MISS') and a.priority";
		String sql = "select to_char(start_time, 'yyyymmddhh24miss') start_time,  d.ftp_ip,  d.ftp_port,  d.ftp_user,  d.ftp_pwd,  f.FILE_PATH,  a.task_id,  a.content_sql,  a.order_id,  a.CONECTION_STR,  e.url wsurl,  e.fun_name funcname,a.file_name filename,b.file_edcoding fileedcoding,b.if_tab_head if_tab_head,b.file_separator fileseparator,b.file_extensions fileextensions,b.database_type,b.upload_type,b.compress_type,b.file_size,a.encrypt_key  from sys_sharedata_task a  left join SYS_SHAREDATA_ORDER_NEW b  on a.order_id = b.order_id  left join SYS_SHAREDATA_FTP d  on b.ftp_id = d.ftp_id  left join sys_sharedata_notifysrv e  on e.order_id = a.order_id  left join sys_sharedata_ftp_path f  on b.ftp_id=f.ftp_id  and b.path_id=f.path_id  left join sys_sharedata_relation_so g on a.order_id=g.order_id left join sys_sharedata_server_config h on g.server_id=h.server_id where a.state = 'δִ��'  and a.start_time < sysdate and  a.priority";
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
		String localIP = "δ֪";
		try {
			netAddress = InetAddress.getLocalHost();
			localIP = netAddress.getHostAddress();
		} catch (UnknownHostException e1) {
			logger.error("��ȡ����IP��ַʧ�ܣ�");
		}

		if ("1".equals(modle.trim())) {
			sql = sql + " and (h.server_ip is null or h.server_ip='" + localIP + "')";
		} else if ("2".equals(modle.trim())) {
			sql = sql + " and h.server_ip='" + localIP + "'";
		}
		logger.info("���ȱ��ѯ���:" + sql);
		do {
			try {
				con = Util.getConnection(ThreadPool.URL, ThreadPool.USER, ThreadPool.PASSWD);
				while (con == null) {
					try {
						Thread.sleep(60000l);
						con = Util.getConnection(ThreadPool.URL, ThreadPool.USER, ThreadPool.PASSWD);
					} catch (InterruptedException e) {
						// ��������
					}

				}
				st = con.createStatement();
				rs = st.executeQuery(sql);
				long item_count = 0L;
				long task_count = 0L;
				
				while (rs.next()) {
					item_count ++;
					try {
						String sqlmetainfo = rs.getString("conection_str");// ��ȡ����Դ
						Blob bl = rs.getBlob("content_sql");// sql���
						BufferedReader bf = new BufferedReader(new InputStreamReader(bl.getBinaryStream(),ThreadPool.DbEncouding));
						String temp = "";
						StringBuffer strsql = new StringBuffer();
						while ((temp = bf.readLine()) != null) {
							strsql.append(temp);
						}

						// taskid = rs.getBigDecimal("task_id").toBigInteger();
						taskid = rs.getString("task_id");
						upsql = "update sys_sharedata_task set state='ִ����'  where task_id='" + taskid + "'";
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
						// �����ļ�������׺����������ݿ����
						String fileName = rs.getString("filename");
						String databaseType = rs.getString("database_type");
						if (StringUtils.isBlank(databaseType)) {
							databaseType = "��HIVE";
						}
						String uploadType = rs.getString("upload_type");
						if (StringUtils.isBlank(uploadType)) {
							uploadType = "0";// ����Ĭ�Ϸ�ʽ�����ϱ�
						}
						String compressType = rs.getString("compress_type");
						if (StringUtils.isBlank(compressType)) {
							compressType = "0";// Ĭ�ϲ�ѹ��
						}
						String fileSize = rs.getString("file_size");
						if (StringUtils.isBlank(fileSize)) {
							fileSize = "";// Ĭ�ϲ��ָ�
						}
						String encryptKey = rs.getString("encrypt_key");
						if (StringUtils.isBlank(encryptKey)) {
							encryptKey = "";// Ĭ�ϲ�����
						}

						String fileedcoding = rs.getString("fileedcoding");
						// �Ƿ��б�ͷ
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
						// System.err.println("�޸Ĳ�����" + fileedcoding + " " +
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
							
							logger.info("��������ļ���ǰ׺:"+fileName);
							task_count ++;
							
						} else {
							Util.Finsh(taskid, "SQL����ǿյ� ����conection_str�ǿ�");
							Util.WebService(wsurl, namespace, funcname, orderid, 2 + "", "��SQL������������Դ��Ϣ ������������", url,
									port + "", username, password, "utf-8", 2 + "", "NULL", remotePath, date, "|");
						}

					} catch (SQLException e) {
						logger.warn("�ڻ�ȡ���ݿ�������Ϣʱ�� ����SQL�쳣 ���쳣��ThreadPoolManger�з��� �����ַ��� " + ThreadPool.URL + ","
								+ ThreadPool.USER + "," + ThreadPool.PASSWD + " SQL���Ϊ : " + sql + "����SQL���:" + upsql,
								e);
						Util.Finsh(taskid, "�ڻ�ȡ���ݿ�������Ϣʱ�� ����SQL�쳣 ���쳣��ThreadPoolManger�з��� �����ַ��� " + ThreadPool.URL + ","
								+ ThreadPool.USER + "," + ThreadPool.PASSWD + " SQL���Ϊ : " + sql + "����SQL���:" + upsql + e.getMessage());

					}
				}
				logger.info("����ִ�����,��ѯ��¼����"+item_count+", �����������"+task_count);

				con.commit();
				Util.release(rs, st, con);
			} catch (SQLException e) {
				Util.release(rs, st, con);
				logger.warn("�ڻ�ȡ���ݿ�������Ϣʱ�� ����SQL�쳣 ���쳣��ThreadPoolManger�з��� �����ַ���" + ThreadPool.URL + ","
						+ ThreadPool.USER + "," + ThreadPool.PASSWD + " SQL���Ϊ : " + sql + "����SQL���" + upsql, e);
				Util.Finsh(taskid, "�ڻ�ȡ���ݿ�������Ϣʱ�� ����SQL�쳣 ���쳣��ThreadPoolManger�з��� �����ַ���" + ThreadPool.URL + ","
						+ ThreadPool.USER + "," + ThreadPool.PASSWD + " SQL���Ϊ : " + sql + "����SQL���" + upsql + e.getMessage());
			} catch (IOException e) {
				Util.release(rs, st, con);
				logger.warn("��ȡSQL��BLOB�ֶ�ʱ����IO�쳣  SQL���Ϊ  " + sql + "�쳣", e);
				Util.Finsh(taskid, "��ȡSQL��BLOB�ֶ�ʱ����IO�쳣  SQL���Ϊ  " + sql + e.getMessage());
			} catch (NullPointerException e) {
				Util.release(rs, st, con);
				logger.warn("��ָ��", e);
				Util.Finsh(taskid, "�����ڲ�����������ָ�� :"+e.getMessage());
			}
			try {
				Thread.sleep(60000l);
			} catch (InterruptedException e) {
				// ��������
			}
			upsql = "";
		} while (true);

	}
}
