package Cn.Boco.SonTask;

import java.io.BufferedWriter;
import java.io.File;
//import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import Cn.Boco.Untils.DESUtil;
import Cn.Boco.Untils.FileUtils;
import Cn.Boco.Untils.Util;
import Cn.Boco.Untils.hiveExp;

public class SonThread_Task extends Task {
	public static Logger logger = Logger.getLogger(SonThread_Task.class);
	public static SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");//�������ڸ�ʽ

	private String sql = "";// sql���
	private String sqlmetainfo = "";
	private String taskid;
	private int if_tab_head = 0;
	private String start_time;

	public String getStart_time() {
		return start_time;
	}

	public void setStart_time(String startTime) {
		start_time = startTime;
	}
	// ���ñ�ͷ

	public int getIf_tab_head() {
		return if_tab_head;
	}

	public void setIf_tab_head(int ifTabHead) {
		if_tab_head = ifTabHead;
	}

	public String getTaskid() {
		return taskid;
	}

	public void setTaskid(String taskid) {
		this.taskid = taskid;
	}

	private static Pattern pattern = Pattern.compile("\\|");
	private String servicedesc = "";
	// private String
	private String url = "";
	private int port = 0;
	private String username = "";
	private String password = "";
	private String remotePath = "";
	private String fileName = "";
	public static final String LOCAL_PATH = "/";
	public String WSurl = "";
	public String namespace = "";
	public String funcname = "";
	public String orderid = "";
	public String fileedcoding = "";
	public String fileseparator = "";
	public String fileextensions = "";
	public String uploadType = "";
	public String compressType = "";
	public String databaseType = "";
	public String tmpPath = "tmpdata/";
	public String fileSize = "";
	public String encryptKey = "";

	public String getFileextensions() {
		return fileextensions;
	}

	public void setFileextensions(String file_extensions) {
		this.fileextensions = file_extensions;
	}

	public String getFileseparator() {
		return fileseparator;
	}

	public void setFileseparator(String file_separator) {
		this.fileseparator = file_separator;
	}

	public String getFile_edcoding() {
		return fileedcoding;
	}

	public void setFileedcoding(String file_edcoding) {
		this.fileedcoding = file_edcoding;
	}

	public String getOrderid() {
		return orderid;
	}

	public void setOrderid(String orderid) {
		this.orderid = orderid;
	}

	public SonThread_Task() {

	}

	public void run() {
		logger.info("��ʼ������"+fileName);
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			String[] sts = sqlmetainfo.split("#");
			// ֮������Ҫע����oracle��������SQL�������⡣
			String driver = sts[0].trim().substring(8);
			String url = sts[1].substring(5);
			String username = sts[2].substring(6);
			String password = sts[3].substring(10);
			// System.out.println("one start");

			if ("org.apache.hive.jdbc.HiveDriver".equals(driver)) {
				url=url+"?characterEncoding=utf-8&tcpRcvBuf=10240000";
			}//20170118�޸����ӻ�������СΪ10m

			logger.info("��ȡ���ݿ����ӣ�"+url);

			con = Util.getConnection(driver, url, username, password);
			if (con == null) {
				Util.Finsh(taskid, "Can not Connnect to DB  because url is bad format or DB is closed : url" + url
						+ "       username:" + username + "       password:" + password);
				return;
			}
			Util.Start(taskid);

			List<String> files = new ArrayList<String>();
			boolean bool = false;
			Long item_count = 0L;
			Long cur_time = System.currentTimeMillis();
			String err_mssage = null; //��ȡ�쳣��Ϣ���������µ���sys_sharedata_task,20170209
			
			logger.info("�ϴ�����Ϊ��"+uploadType);
			// ���������ִ�У�upload_type=1 �����ϱ���=2 �㽭�����ϱ� �������ͨ�ϱ�
			// �㽭������������ȫ��ͬ�����⴦��
			
			if (uploadType.equals("2")) {

				logger.info("ִ��2��������");
				// �����ϱ���ʹ��hive��ʽ
				st = con.createStatement();
				if ("org.apache.hive.jdbc.HiveDriver".equals(driver)) {
					st.execute("use " + username);
					logger.info("use " + username);
				}
				rs = st.executeQuery(sql);
				ResultSetMetaData data = rs.getMetaData();
				int colum = rs.getMetaData().getColumnCount();// ��ȡ����

				// //��ȡ��ͷ�Ĳ���
				// StringBuffer tab_head=Util.GetTableHead(data);

				String localFile = tmpPath + fileName + ".tmp.unencrypt";
				BufferedWriter bw = new BufferedWriter(
						new OutputStreamWriter(new FileOutputStream(localFile), fileedcoding));
				// д�ļ�
				// xml�ļ�ͷ
				bw.write("<?xml version='1.0' encoding='utf-8'?>\n");
				bw.write("<Root type='01'>\n");
				bw.write("  <Result>\n");

				while (rs.next()) {
					item_count++;
					for (int i = 1; i <= colum; i++) {
						String cellValue = "";
						if (rs.getObject(i) != null) {
							cellValue = rs.getObject(i).toString().trim();
						}
						if ("null".equalsIgnoreCase(cellValue)) {
							cellValue = "";
						}
						String column = data.getColumnName(i);
						// д�ļ�ǰ�����ͺ���ĸ�ʽ��һ��
						if (i < 7) {
							bw.write("    <" + column + ">" + cellValue + "</" + column + ">");
							bw.write("\n");
						} else if (i == 7) {
							bw.write("    <Record>\n");
							bw.write("      <" + column + ">" + cellValue + "</" + column + ">");
							bw.write("\n");
						} else {
							bw.write("      <" + column + ">" + cellValue + "</" + column + ">");
							bw.write("\n");
						}
					}
					bw.write("    </Record>\n");
				}
				bw.write("  </Result>\n");
				bw.write("</Root>\n");
				// xml��β
				bw.close();
				logger.info("����xml�ļ���" + localFile);
				// д�ļ���������ݼ���
				String encryptFile = localFile.replace(".unencrypt", "");
				// ����key��ʽΪ ��Ȩ��|��Կ
				String authCode = "";
				String encKey = "";

				if (StringUtils.isNotBlank(encryptKey)) {
					String[] codekey = pattern.split(encryptKey, -1);
					if (codekey.length > 1) {
						authCode = codekey[0];
						encKey = codekey[1];
					} else {
						encKey = codekey[0];
					}
				}

				// �м���KEY���м��ܣ�û��������ȥ��δ����
				if (StringUtils.isNotBlank(encKey)) {
					DESUtil.encryptFile(localFile, encryptFile, encKey);
					logger.info("���xml�ļ����ܣ�" + encryptFile);
				} else {
					FileUtils.renameFile(localFile, encryptFile);
					logger.info("�޼���key,�������ļ���" + encryptFile);
				}

				bool = Util.uploadFileChangeNane(this.url, port, this.username, this.password,
						new String(this.remotePath.getBytes("utf-8"), "iso-8859-1"), encryptFile, taskid, fileedcoding);

				logger.info("��ɼ����ļ��ϴ���" + encryptFile);
				File file = new File(encryptFile);
				String fileName = file.getName().replace(".tmp", ".dat");
				String filePath = this.remotePath;
				String localfileSize = String.valueOf(file.length());
				// �ϴ���ɣ�ɾ�������ļ�
				FileUtils.deleteFile(encryptFile);
				// �㽭���ⵥ��webservice��ʽ
				Util.WebService(this.WSurl, this.namespace, this.funcname, authCode, fileName, filePath, localfileSize,
						item_count.toString());
				// ������ɺ����webservice��������
				this.WSurl = "";
			} else {
				Long limitSize = 0L;
				String tableName = "";
				StringBuffer tab_head = new StringBuffer();
				if (uploadType.equals("1")) {
					// �����ļ��ϱ���СĬ��Ϊ100M
					limitSize = StringUtils.isBlank(fileSize) || fileSize.equals("0") ? 100 * 1024 * 1024
							: Long.parseLong(fileSize) * 1024 * 1024;
					// �����ļ����Ʋ�ƴ��ORDER_ID
					tableName = fileName.toLowerCase();
					logger.info(fileName + "ִ��1��������");
				} else if (uploadType.equals("0")) {
					// һ�㹲�����ô�С���ߴ�СΪ0�����з�ҳ����
					limitSize = StringUtils.isBlank(fileSize) ? 0L : Long.parseLong(fileSize) * 1024 * 1024;
					// һ�㹲���ļ�����ƴ��ORDER_ID
					if (StringUtils.isNotBlank(fileName)) {
						tableName = (fileName + this.orderid).toLowerCase();
						fileName = fileName + this.orderid;
					} else {
						// ���û�п�ʼʱ���ֱ��ʹ��ϵͳ�ĵ�ǰʱ�䡣
						tableName = this.orderid.toLowerCase();
						fileName = this.orderid+df.format(new Date());
					}
					logger.info(fileName + ",ִ��0��������," + "limitSize�Ĵ�С " + limitSize);
				}
				
				if (limitSize == 0) {
					if ("hive".equals(databaseType.toLowerCase())) {
						st = con.createStatement();
						st.execute("use " + username);
						logger.info("use " + username);
						try{//�޸�hive�����ӿ�סbug�����˳�ʱ��trycatch
						// ��ȡ��ͷ
						if (if_tab_head == 1) {
							rs = st.executeQuery(sql.replaceAll("(?i)where", "where 1=2 and "));
							ResultSetMetaData data = rs.getMetaData();
							tab_head = Util.GetTableHead(data,fileseparator);
							logger.info("��ͷ:" + tab_head);
						}
						// ������ʱ��洢����
						logger.info(fileName + "������ʱ�����ݣ�create table tmp.tmp" + tableName
								+ " ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + fileseparator
								+ "' TBLPROPERTIES('serialization.null.format' = '') as " + sql);
						// �����ж���ʱ���Ƿ���ڣ�����ɾ��,Ȼ�󴴽�
						st.execute("drop table if exists tmp.tmp" + tableName);
						st.execute("create table tmp.tmp" + tableName + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '"
								+ fileseparator + "' TBLPROPERTIES('serialization.null.format' = '') as " + sql);
						} catch (Exception e)  {
							logger.info(e);
							err_mssage = e.getMessage();
						}
						st.close();
						con.close();
						logger.info(fileName + "�����������");
						// ��hdfs��ȡ����
						files = hiveExp.getFilePage("/user/hive/warehouse/tmp.db/tmp" + tableName, tmpPath + fileName,
								fileedcoding, fileextensions, tab_head.toString(), if_tab_head);
						for (String file : files) {
							logger.info(fileName + "�Ѿ������ļ���" + file);
						}
						// ɾ����ʱ��
						logger.info(fileName + "��ʼɾ����ʱ��tmp.tmp" + tableName);
						con = Util.getConnection(driver, url, username, password);
						st = con.createStatement();
						st.execute("drop table tmp.tmp" + tableName);
						st.close();
						con.close();
						logger.info(fileName + "��ʱ��ɾ�����");
					} else {
						st = con.createStatement();
						// ִ����������Դʹ��JDBC����
						if ("org.apache.hive.jdbc.HiveDriver".equals(driver)) {
							st.execute("use " + username);
							logger.info("use " + username);
						}
						
						st.setFetchSize(200);
						
						rs = st.executeQuery(sql);
						
						rs.setFetchSize(400);
						
						ResultSetMetaData data = rs.getMetaData();
						int colum = data.getColumnCount();// ��ȡ����
						if (if_tab_head == 1) {
							tab_head = Util.GetTableHead(data,fileseparator);
							logger.info("��ͷ" + tab_head + " �Ƿ�ʹ�ñ�ͷ: " + if_tab_head);
						}
						String localFile = tmpPath + fileName + "." + fileextensions;
						BufferedWriter bw = new BufferedWriter(
								new OutputStreamWriter(new FileOutputStream(localFile), fileedcoding));
						int j = 0;// ��ͷֻ�����һ��
						if (if_tab_head == 1 && j == 0 && tab_head != null) {
							bw.write(tab_head.toString() + "\n");
							j++;
						}
						while (rs.next()) {
							item_count ++;
							for (int i = 1; i <= colum; i++) {
								String cellValue = "";
								if (rs.getObject(i) != null) {
									cellValue = rs.getObject(i).toString().trim();
								}
								if ("null".equalsIgnoreCase(cellValue)) {
									cellValue = "";
								}
								if (i == colum) {

									bw.write(cellValue + "\n");
								} else {
									bw.write(cellValue + fileseparator);
								}
							}
						}
						bw.close();
						files.add(localFile);
					}
				} else {// ���з�ҳ����
					if ("hive".equals(databaseType.toLowerCase())) {
						st = con.createStatement();
						st.execute("use " + username);
						logger.info("use " + username);
						// ��ȡ��ͷ
						if (if_tab_head == 1) {
							logger.info("��ʼ��ȡ��ͷ");
							rs = st.executeQuery(sql.replaceAll("(?i)where", "where 1=2 and "));
							ResultSetMetaData data = rs.getMetaData();
							tab_head = Util.GetTableHead(data,fileseparator);
							logger.info("��ͷ��" + tab_head);
						}

						// ������ʱ��洢����
						// �����ж���ʱ���Ƿ���ڣ�����ɾ��,Ȼ�󴴽�
						st.execute("drop table if exists tmp.tmp" + tableName);
						logger.info("ɾ����ʱ����ɣ�tmp.tmp" + tableName);

						logger.info(fileName + "��ʼ������ʱ�����ݣ�create table tmp.tmp" + tableName
								+ " ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + fileseparator
								+ "' TBLPROPERTIES('serialization.null.format' = '') as " + sql);
						// st.setQueryTimeout(3600);
						st.execute("create table tmp.tmp" + tableName + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '"
								+ fileseparator + "' TBLPROPERTIES('serialization.null.format' = '') as " + sql);

						logger.info(fileName + "������ʱ���������");
						Util.release(rs, st, con);

						// ��hdfs��ȡ����, ���з��ļ����ж��Ƿ���Ҫѹ��
						files = hiveExp.getFilePage("/user/hive/warehouse/tmp.db/tmp" + tableName, tmpPath + fileName,
								fileedcoding, limitSize, fileextensions, tab_head.toString(), if_tab_head);
						for (String file : files) {
							logger.info(fileName + "�Ѿ������ļ���" + file);
						}
						// ɾ����ʱ��
						logger.info(fileName + "��ʼɾ����ʱ��tmp.tmp" + tableName);
						con = Util.getConnection(driver, url, username, password);
						st = con.createStatement();
						st.execute("drop table tmp.tmp" + tableName);
						st.close();
						con.close();
						logger.info(fileName + "��ʱ��ɾ�����");
					} else {
						st = con.createStatement();
						// ִ����������Դʹ��JDBC����
						if ("org.apache.hive.jdbc.HiveDriver".equals(driver)) {
							st.execute("use " + username);
							logger.info("use " + username);
						}

						rs = st.executeQuery(sql);
						// ��ȡ��ͷ
						ResultSetMetaData data = rs.getMetaData();
						
						int colum = data.getColumnCount();// ��ȡ����
						if (if_tab_head == 1) {
							tab_head = Util.GetTableHead(data,fileseparator);
						}
					
						int page = 101;
						String localFile = tmpPath + fileName + "_P" + String.valueOf(page).substring(1, 3) + "."
								+ fileextensions;
						BufferedWriter bw = new BufferedWriter(
								new OutputStreamWriter(new FileOutputStream(localFile), fileedcoding));
						if (if_tab_head == 1 && tab_head != null) {
							bw.write(tab_head.toString());
						}
						Long size = 0L;
						// int k = 0;
						/*
						 * if (if_tab_head == 1 && k == 0 && tab_head != null) {
						 * bw.write(tab_head.toString() + "\n"); k++;
						 * 
						 * }
						 */
						
						
						while (rs.next()) {
							item_count ++;
							for (int i = 1; i <= colum; i++) {
								String cellValue = "";
								if (rs.getObject(i) != null) {
									cellValue = rs.getObject(i).toString().trim();
								}
								if ("null".equalsIgnoreCase(cellValue)) {
									cellValue = "";
								}
								if (i == colum) {
									/*
									 * if (if_tab_head == 1 && k == 0 &&
									 * tab_head != null) {
									 * bw.write(tab_head.toString() + "\n");
									 * k++;
									 * 
									 * }
									 */
									bw.write(cellValue + "\n");
									size += (cellValue + "\n").getBytes().length;
								} else {
									bw.write(cellValue + fileseparator);
									size += (cellValue + fileseparator).getBytes().length;
								}
							}
							if (size >= limitSize) {
								// ���ӵ��б���ȥ
								bw.close();
								// ��ͷ������޸�
//								if (if_tab_head == 1 && tab_head != null) {
//									logger.info("��ͷ " + tab_head + "�ļ�����" + localFile);
//									hiveExp.insertNewLine(localFile, tab_head.toString(), 0, fileedcoding);
//								}
								files.add(localFile);
								// ��ʼ��
								page++;
								size = 0L;
								localFile = tmpPath + fileName + "_P" + String.valueOf(page).substring(1, 3) + "."
										+ fileextensions;
								bw = new BufferedWriter(
										new OutputStreamWriter(new FileOutputStream(localFile), fileedcoding));
								if (if_tab_head == 1 && tab_head != null) {
									bw.write(tab_head.toString());
								}
								// k = 0;
							}
						}
						Util.release(rs, st, con);
						// �������һ���ļ�
						bw.close();
						// ��ͷ������޸�
						/*if (if_tab_head == 1 && tab_head != null) {
							logger.info("��ͷ " + tab_head + "�ļ�����" + localFile);
							hiveExp.insertNewLine(localFile, tab_head.toString(), 0, fileedcoding);
						}*/
						// ���һ���ļ�
						String endFile = localFile.replace("." + fileextensions, "_END." + fileextensions);
						FileUtils.renameFile(localFile, endFile);
						logger.info("������һ���ļ���������" + endFile);
						files.add(endFile);
						
					}
					

				}
				
				logger.info("���ݵ����ɹ�,������¼����"+item_count+",�����ļ�������"+files.size()+",��ʱ��"+(System.currentTimeMillis()-cur_time)/1000.0 + "s");
				// �����ɵ��ļ����м��ܴ����ѹ�����������ļ��ϴ�
				logger.info("�ϴ��ļ�����"+this.url+this.remotePath);
				for (String file : files) {
					if (StringUtils.isNotBlank(encryptKey)) {
						try {
							DESUtil.encryptFile(file, file + ".enc", encryptKey);
							FileUtils.renameFile(file + ".enc", file);
							logger.info(file + "��ɼ��ܣ�");
						} catch (Exception e) {
							e.printStackTrace();
							err_mssage = e.getMessage();
						}
					}
					if (compressType.equals("gz")) {
						file = FileUtils.compressFile(file);
						logger.info(file + "���ѹ����");
					}

					bool = Util.uploadFile(this.url, port, this.username, this.password,
							new String(this.remotePath.getBytes("utf-8"), "iso-8859-1"), file, taskid, fileedcoding);
					// logger.info("����ļ��ϴ���" + file);
					// �ϴ���ɺ�ɾ���ļ�
					FileUtils.deleteFile(file);
				}
			}
			if (!bool) {
				Util.Finsh(taskid, "Can not upload file to ftp servier  more info : url:" + this.url + " port:" + port
						+ " username:" + this.username + " password:" + this.password + " remotepath:" + remotePath);
				logger.info(fileName + "�ļ��ϴ� ʧ��");

				Util.WebService(this.WSurl, this.namespace, this.funcname, orderid, 2 + "", "�����FTP��Ϣ�������޷���ɱ��η���",
						this.url, this.port + "", this.username, this.password, "utf-8", 2 + "", "NULL",
						this.remotePath, Util.getData(), "|");

			} else if(err_mssage != null){
				Util.Finsh(taskid, err_mssage);
			}else {
				Util.Finsh(taskid, "SUCCESS");
				logger.info(fileName + "�ļ��ϴ��ɹ�");

				Util.WebService(this.WSurl, this.namespace, this.funcname, orderid, 0 + "", "�ɹ�", this.url,
						this.port + "", this.username, this.password, "utf-8", 2 + "", "NULL", this.remotePath,
						Util.getData(), "|");

			}
		} catch (IOException e) {
			Util.release(rs, st, con);
			logger.warn("����ID" + taskid + "   ���߳�������ִ������ʱ�� ������Ϊ�ļ�����������IO�쳣 ���±�����������Ч���", e);
			Util.Finsh(taskid, "����ID" + taskid + e.getMessage());
			Util.WebService(this.WSurl, this.namespace, this.funcname, orderid, 2 + "",
					"���߳�������ִ������ʱ�� ������Ϊ�ļ�����������IO�쳣 ���±�����������Ч���", this.url, this.port + "", this.username, this.password,
					"utf-8", 2 + "", "NULL", this.remotePath, Util.getData(), "|");
		} catch (SQLException e) {
			Util.release(rs, st, con);
			logger.warn("����ID" + taskid + "   ���߳�������ִ������ʱ����Ϊ���ݿ��쳣���޷���ȷ��Ŀ�����ݿ��ȡ�Լ�Ҫ���ؼ�¼������  �쳣��ϢΪ ", e);
			Util.Finsh(taskid, "����ID" + taskid + "   ���߳�������ִ������ʱ����Ϊ���ݿ��쳣���޷���ȷ��Ŀ�����ݿ��ȡ�Լ�Ҫ���ؼ�¼������  ���鱾��������������ݿ��ַ��� "+ e.getMessage());
			Util.WebService(this.WSurl, this.namespace, this.funcname, orderid, 2 + "",
					"���߳�������ִ������ʱ����Ϊ���ݿ��쳣���޷���ȷ��Ŀ�����ݿ��ȡ�Լ�Ҫ���ؼ�¼������  ���鱾��������������ݿ��ַ��� ", this.url, this.port + "",
					this.username, this.password, "utf-8", 2 + "", "NULL", this.remotePath, Util.getData(), "|");
		} catch (Exception e) {
			Util.release(rs, st, con);
			logger.warn("����ID" + taskid + "   ����δ֪�쳣  �쳣��ϢΪ ", e);
			Util.Finsh(taskid, "����ID" + taskid + "   ����δ֪�쳣  �쳣��ϢΪ " + e.getMessage());
			Util.WebService(this.WSurl, this.namespace, this.funcname, orderid, 2 + "", "����δ֪�쳣  �쳣��ϢΪ " + e.getMessage(),
					this.url, this.port + "", this.username, this.password, "utf-8", 2 + "", "NULL", this.remotePath,
					Util.getData(), "|");
		}
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getSqlmetainfo() {
		return sqlmetainfo;
	}

	public void setSqlmetainfo(String sqlmetainfo) {
		this.sqlmetainfo = sqlmetainfo;
	}

	public String getServicedesc() {
		return servicedesc;
	}

	public void setServicedesc(String servicedesc) {
		this.servicedesc = servicedesc;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getRemotePath() {
		return remotePath;
	}

	public void setRemotePath(String remotePath) {
		this.remotePath = remotePath;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getWSurl() {
		return WSurl;
	}

	public void setWSurl(String wSurl) {
		WSurl = wSurl;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getFuncname() {
		return funcname;
	}

	public void setFuncname(String funcname) {
		this.funcname = funcname;
	}

	public String getDatabaseType() {
		return databaseType;
	}

	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}

	public String getUploadType() {
		return uploadType;
	}

	public void setUploadType(String uploadType) {
		this.uploadType = uploadType;
	}

	public String getCompressType() {
		return compressType;
	}

	public void setCompressType(String compressType) {
		this.compressType = compressType;
	}

	public String getFileSize() {
		return fileSize;
	}

	public void setFileSize(String fileSize) {
		this.fileSize = fileSize;
	}

	public String EncryptKey() {
		return encryptKey;
	}

	public void setEncryptKey(String encryptKey) {
		this.encryptKey = encryptKey;
	}

}
