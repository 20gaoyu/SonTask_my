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
	public static SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");//设置日期格式

	private String sql = "";// sql语句
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
	// 设置表头

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
		logger.info("开始导出："+fileName);
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			String[] sts = sqlmetainfo.split("#");
			// 之后这里要注意是oracle驱动还是SQL驱动问题。
			String driver = sts[0].trim().substring(8);
			String url = sts[1].substring(5);
			String username = sts[2].substring(6);
			String password = sts[3].substring(10);
			// System.out.println("one start");

			if ("org.apache.hive.jdbc.HiveDriver".equals(driver)) {
				url=url+"?characterEncoding=utf-8&tcpRcvBuf=10240000";
			}//20170118修改增加缓冲区大小为10m

			logger.info("获取数据库连接："+url);

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
			String err_mssage = null; //获取异常信息，用来更新到表sys_sharedata_task,20170209
			
			logger.info("上传类型为："+uploadType);
			// 分三种情况执行，upload_type=1 集团上报，=2 浙江拨测上报 其余的普通上报
			// 浙江拨测与其它完全不同，特殊处理
			
			if (uploadType.equals("2")) {

				logger.info("执行2类型数据");
				// 拨测上报不使用hive方式
				st = con.createStatement();
				if ("org.apache.hive.jdbc.HiveDriver".equals(driver)) {
					st.execute("use " + username);
					logger.info("use " + username);
				}
				rs = st.executeQuery(sql);
				ResultSetMetaData data = rs.getMetaData();
				int colum = rs.getMetaData().getColumnCount();// 获取列数

				// //获取表头的操作
				// StringBuffer tab_head=Util.GetTableHead(data);

				String localFile = tmpPath + fileName + ".tmp.unencrypt";
				BufferedWriter bw = new BufferedWriter(
						new OutputStreamWriter(new FileOutputStream(localFile), fileedcoding));
				// 写文件
				// xml文件头
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
						// 写文件前六个和后面的格式不一样
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
				// xml结尾
				bw.close();
				logger.info("生成xml文件：" + localFile);
				// 写文件后进行数据加密
				String encryptFile = localFile.replace(".unencrypt", "");
				// 拨测key格式为 鉴权码|秘钥
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

				// 有加密KEY进行加密，没有重命名去掉未加密
				if (StringUtils.isNotBlank(encKey)) {
					DESUtil.encryptFile(localFile, encryptFile, encKey);
					logger.info("完成xml文件加密：" + encryptFile);
				} else {
					FileUtils.renameFile(localFile, encryptFile);
					logger.info("无加密key,重命名文件：" + encryptFile);
				}

				bool = Util.uploadFileChangeNane(this.url, port, this.username, this.password,
						new String(this.remotePath.getBytes("utf-8"), "iso-8859-1"), encryptFile, taskid, fileedcoding);

				logger.info("完成加密文件上传：" + encryptFile);
				File file = new File(encryptFile);
				String fileName = file.getName().replace(".tmp", ".dat");
				String filePath = this.remotePath;
				String localfileSize = String.valueOf(file.length());
				// 上传完成，删除加密文件
				FileUtils.deleteFile(encryptFile);
				// 浙江拨测单独webservice格式
				Util.WebService(this.WSurl, this.namespace, this.funcname, authCode, fileName, filePath, localfileSize,
						item_count.toString());
				// 发送完成后面的webservice不做操作
				this.WSurl = "";
			} else {
				Long limitSize = 0L;
				String tableName = "";
				StringBuffer tab_head = new StringBuffer();
				if (uploadType.equals("1")) {
					// 集团文件上报大小默认为100M
					limitSize = StringUtils.isBlank(fileSize) || fileSize.equals("0") ? 100 * 1024 * 1024
							: Long.parseLong(fileSize) * 1024 * 1024;
					// 集团文件名称不拼接ORDER_ID
					tableName = fileName.toLowerCase();
					logger.info(fileName + "执行1类型数据");
				} else if (uploadType.equals("0")) {
					// 一般共享不配置大小或者大小为0不进行分页处理
					limitSize = StringUtils.isBlank(fileSize) ? 0L : Long.parseLong(fileSize) * 1024 * 1024;
					// 一般共享文件名称拼接ORDER_ID
					if (StringUtils.isNotBlank(fileName)) {
						tableName = (fileName + this.orderid).toLowerCase();
						fileName = fileName + this.orderid;
					} else {
						// 如果没有开始时间就直接使用系统的当前时间。
						tableName = this.orderid.toLowerCase();
						fileName = this.orderid+df.format(new Date());
					}
					logger.info(fileName + ",执行0类型数据," + "limitSize的大小 " + limitSize);
				}
				
				if (limitSize == 0) {
					if ("hive".equals(databaseType.toLowerCase())) {
						st = con.createStatement();
						st.execute("use " + username);
						logger.info("use " + username);
						try{//修改hive、连接卡住bug，加了超时和trycatch
						// 获取表头
						if (if_tab_head == 1) {
							rs = st.executeQuery(sql.replaceAll("(?i)where", "where 1=2 and "));
							ResultSetMetaData data = rs.getMetaData();
							tab_head = Util.GetTableHead(data,fileseparator);
							logger.info("表头:" + tab_head);
						}
						// 创建临时表存储数据
						logger.info(fileName + "插入临时表数据：create table tmp.tmp" + tableName
								+ " ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + fileseparator
								+ "' TBLPROPERTIES('serialization.null.format' = '') as " + sql);
						// 首先判断临时表是否存在，存在删除,然后创建
						st.execute("drop table if exists tmp.tmp" + tableName);
						st.execute("create table tmp.tmp" + tableName + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '"
								+ fileseparator + "' TBLPROPERTIES('serialization.null.format' = '') as " + sql);
						} catch (Exception e)  {
							logger.info(e);
							err_mssage = e.getMessage();
						}
						st.close();
						con.close();
						logger.info(fileName + "插入数据完成");
						// 从hdfs获取数据
						files = hiveExp.getFilePage("/user/hive/warehouse/tmp.db/tmp" + tableName, tmpPath + fileName,
								fileedcoding, fileextensions, tab_head.toString(), if_tab_head);
						for (String file : files) {
							logger.info(fileName + "已经生成文件：" + file);
						}
						// 删除临时表
						logger.info(fileName + "开始删除临时表：tmp.tmp" + tableName);
						con = Util.getConnection(driver, url, username, password);
						st = con.createStatement();
						st.execute("drop table tmp.tmp" + tableName);
						st.close();
						con.close();
						logger.info(fileName + "临时表删除完成");
					} else {
						st = con.createStatement();
						// 执行其他数据源使用JDBC连接
						if ("org.apache.hive.jdbc.HiveDriver".equals(driver)) {
							st.execute("use " + username);
							logger.info("use " + username);
						}
						
						st.setFetchSize(200);
						
						rs = st.executeQuery(sql);
						
						rs.setFetchSize(400);
						
						ResultSetMetaData data = rs.getMetaData();
						int colum = data.getColumnCount();// 获取列数
						if (if_tab_head == 1) {
							tab_head = Util.GetTableHead(data,fileseparator);
							logger.info("表头" + tab_head + " 是否使用表头: " + if_tab_head);
						}
						String localFile = tmpPath + fileName + "." + fileextensions;
						BufferedWriter bw = new BufferedWriter(
								new OutputStreamWriter(new FileOutputStream(localFile), fileedcoding));
						int j = 0;// 表头只用输出一次
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
				} else {// 进行分页操作
					if ("hive".equals(databaseType.toLowerCase())) {
						st = con.createStatement();
						st.execute("use " + username);
						logger.info("use " + username);
						// 获取表头
						if (if_tab_head == 1) {
							logger.info("开始获取表头");
							rs = st.executeQuery(sql.replaceAll("(?i)where", "where 1=2 and "));
							ResultSetMetaData data = rs.getMetaData();
							tab_head = Util.GetTableHead(data,fileseparator);
							logger.info("表头：" + tab_head);
						}

						// 创建临时表存储数据
						// 首先判断临时表是否存在，存在删除,然后创建
						st.execute("drop table if exists tmp.tmp" + tableName);
						logger.info("删除临时表完成：tmp.tmp" + tableName);

						logger.info(fileName + "开始插入临时表数据：create table tmp.tmp" + tableName
								+ " ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + fileseparator
								+ "' TBLPROPERTIES('serialization.null.format' = '') as " + sql);
						// st.setQueryTimeout(3600);
						st.execute("create table tmp.tmp" + tableName + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '"
								+ fileseparator + "' TBLPROPERTIES('serialization.null.format' = '') as " + sql);

						logger.info(fileName + "插入临时表数据完成");
						Util.release(rs, st, con);

						// 从hdfs获取数据, 进行分文件，判断是否需要压缩
						files = hiveExp.getFilePage("/user/hive/warehouse/tmp.db/tmp" + tableName, tmpPath + fileName,
								fileedcoding, limitSize, fileextensions, tab_head.toString(), if_tab_head);
						for (String file : files) {
							logger.info(fileName + "已经生成文件：" + file);
						}
						// 删除临时表
						logger.info(fileName + "开始删除临时表：tmp.tmp" + tableName);
						con = Util.getConnection(driver, url, username, password);
						st = con.createStatement();
						st.execute("drop table tmp.tmp" + tableName);
						st.close();
						con.close();
						logger.info(fileName + "临时表删除完成");
					} else {
						st = con.createStatement();
						// 执行其他数据源使用JDBC连接
						if ("org.apache.hive.jdbc.HiveDriver".equals(driver)) {
							st.execute("use " + username);
							logger.info("use " + username);
						}

						rs = st.executeQuery(sql);
						// 获取表头
						ResultSetMetaData data = rs.getMetaData();
						
						int colum = data.getColumnCount();// 获取列数
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
								// 增加到列表中去
								bw.close();
								// 表头问题的修改
//								if (if_tab_head == 1 && tab_head != null) {
//									logger.info("表头 " + tab_head + "文件名：" + localFile);
//									hiveExp.insertNewLine(localFile, tab_head.toString(), 0, fileedcoding);
//								}
								files.add(localFile);
								// 初始化
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
						// 处理最后一个文件
						bw.close();
						// 表头问题的修改
						/*if (if_tab_head == 1 && tab_head != null) {
							logger.info("表头 " + tab_head + "文件名：" + localFile);
							hiveExp.insertNewLine(localFile, tab_head.toString(), 0, fileedcoding);
						}*/
						// 最后一个文件
						String endFile = localFile.replace("." + fileextensions, "_END." + fileextensions);
						FileUtils.renameFile(localFile, endFile);
						logger.info("完成最后一个文件重命名：" + endFile);
						files.add(endFile);
						
					}
					

				}
				
				logger.info("数据导出成功,导出记录数："+item_count+",生成文件个数："+files.size()+",耗时："+(System.currentTimeMillis()-cur_time)/1000.0 + "s");
				// 对生成的文件进行加密处理和压缩处理、进行文件上传
				logger.info("上传文件到："+this.url+this.remotePath);
				for (String file : files) {
					if (StringUtils.isNotBlank(encryptKey)) {
						try {
							DESUtil.encryptFile(file, file + ".enc", encryptKey);
							FileUtils.renameFile(file + ".enc", file);
							logger.info(file + "完成加密！");
						} catch (Exception e) {
							e.printStackTrace();
							err_mssage = e.getMessage();
						}
					}
					if (compressType.equals("gz")) {
						file = FileUtils.compressFile(file);
						logger.info(file + "完成压缩！");
					}

					bool = Util.uploadFile(this.url, port, this.username, this.password,
							new String(this.remotePath.getBytes("utf-8"), "iso-8859-1"), file, taskid, fileedcoding);
					// logger.info("完成文件上传：" + file);
					// 上传完成后删除文件
					FileUtils.deleteFile(file);
				}
			}
			if (!bool) {
				Util.Finsh(taskid, "Can not upload file to ftp servier  more info : url:" + this.url + " port:" + port
						+ " username:" + this.username + " password:" + this.password + " remotepath:" + remotePath);
				logger.info(fileName + "文件上传 失败");

				Util.WebService(this.WSurl, this.namespace, this.funcname, orderid, 2 + "", "错误的FTP信息，所以无法完成本次服务",
						this.url, this.port + "", this.username, this.password, "utf-8", 2 + "", "NULL",
						this.remotePath, Util.getData(), "|");

			} else if(err_mssage != null){
				Util.Finsh(taskid, err_mssage);
			}else {
				Util.Finsh(taskid, "SUCCESS");
				logger.info(fileName + "文件上传成功");

				Util.WebService(this.WSurl, this.namespace, this.funcname, orderid, 0 + "", "成功", this.url,
						this.port + "", this.username, this.password, "utf-8", 2 + "", "NULL", this.remotePath,
						Util.getData(), "|");

			}
		} catch (IOException e) {
			Util.release(rs, st, con);
			logger.warn("任务ID" + taskid + "   子线程任务在执行任务时候 可能因为文件操作而导致IO异常 导致本次任务不能有效完成", e);
			Util.Finsh(taskid, "任务ID" + taskid + e.getMessage());
			Util.WebService(this.WSurl, this.namespace, this.funcname, orderid, 2 + "",
					"子线程任务在执行任务时候 可能因为文件操作而导致IO异常 导致本次任务不能有效完成", this.url, this.port + "", this.username, this.password,
					"utf-8", 2 + "", "NULL", this.remotePath, Util.getData(), "|");
		} catch (SQLException e) {
			Util.release(rs, st, con);
			logger.warn("任务ID" + taskid + "   子线程任务在执行任务时候因为数据库异常而无法正确从目标数据库获取自己要加载记录的数据  异常信息为 ", e);
			Util.Finsh(taskid, "任务ID" + taskid + "   子线程任务在执行任务时候因为数据库异常而无法正确从目标数据库获取自己要加载记录的数据  请检查本条任务关联的数据库字符串 "+ e.getMessage());
			Util.WebService(this.WSurl, this.namespace, this.funcname, orderid, 2 + "",
					"子线程任务在执行任务时候因为数据库异常而无法正确从目标数据库获取自己要加载记录的数据  请检查本条任务关联的数据库字符串 ", this.url, this.port + "",
					this.username, this.password, "utf-8", 2 + "", "NULL", this.remotePath, Util.getData(), "|");
		} catch (Exception e) {
			Util.release(rs, st, con);
			logger.warn("任务ID" + taskid + "   其他未知异常  异常信息为 ", e);
			Util.Finsh(taskid, "任务ID" + taskid + "   其他未知异常  异常信息为 " + e.getMessage());
			Util.WebService(this.WSurl, this.namespace, this.funcname, orderid, 2 + "", "其他未知异常  异常信息为 " + e.getMessage(),
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
