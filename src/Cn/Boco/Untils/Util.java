package Cn.Boco.Untils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
//import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
//import java.math.BigInteger;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.namespace.QName;
import javax.xml.rpc.ServiceException;

import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import Cn.Boco.Manager.ThreadPool;

public class Util {

	public static String encoding = System.getProperty("file.encoding");

	public static Logger logger = Logger.getLogger(Util.class);

	// static {
	// try {
	// Class.forName("oracle.jdbc.driver.OracleDriver");
	// Class.forName("com.mysql.jdbc.Driver");
	// Class.forName("org.apache.hive.jdbc.HiveDriver");
	//
	// } catch (ClassNotFoundException e) {
	// logger.error("没有数据库驱动包"+e);
	//
	// }
	// }
	@SuppressWarnings("static-access")
	public static Connection getConnection(String url, String username, String password) {
		long i = 0;
		Connection connection = null;
		while (connection == null) {
			try {
				if (url == null || "".equals(url)) {
					return null;
				}
				Class.forName("oracle.jdbc.driver.OracleDriver");
				connection = DriverManager.getConnection(url, username, password);
				if (connection != null) {
					i = 0;
					break;
				}
				Thread.currentThread().sleep(10000l);
			} catch (InterruptedException e) {
				Util.logger.warn("此异常为允许异常，在等待获取SQL连接对象时候，强制结束等待状态 再次尝试获取连接", e);
			} catch (SQLException e) {
				i++;
				Util.logger.warn(
						"无法链接到数据库 SQL连接信息为" + url + "用户名" + username + "密码" + password + "异常信息" + e.getMessage(), e);
				if (i % 10 == 0) {
					return null;
				}
			} catch (IllegalArgumentException e) {
				// 此异常一般由implus抛出
				Util.logger.warn(
						"无法链接到数据库 SQL  存在非法的数据库连接字符串 连接信息为" + url + "用户名" + username + "密码" + password + "异常信息", e);
				i++;
				if (i % 5 == 0) {
					return null;
				}
			} catch (ClassNotFoundException e) {
				logger.error("没有数据库驱动包" + e);
				return null;
			}
		}
		return connection;
	}

	@SuppressWarnings("static-access")
	public static Connection getConnection(String driver, String url, String username, String password) {
		long i = 0;
		Connection connection = null;
		while (connection == null) {
			try {
				if (url == null || "".equals(url)) {
					return null;
				}
				Class.forName(driver);
				if(url.contains("principal"))
				{
					Util.logger.info("需要认证kerberos");
					try {
						connection=kerberosIdent(url);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				if(connection == null)
				{
				connection = DriverManager.getConnection(url, username, password);
				}
				if (connection != null) {
					i = 0;
					break;
				}
				Thread.currentThread().sleep(10000l);
			} catch (InterruptedException e) {
				Util.logger.warn("此异常为允许异常，在等待获取SQL连接对象时候，强制结束等待状态 再次尝试获取连接", e);
			} catch (SQLException e) {
				i++;
				Util.logger.warn(
						"无法链接到数据库 SQL连接信息为" + url + "用户名" + username + "密码" + password + "异常信息" + e.getMessage(), e);
				if (i % 10 == 0) {
					return null;
				}
			} catch (IllegalArgumentException e) {
				// 此异常一般由implus抛出
				Util.logger.warn(
						"无法链接到数据库 SQL  存在非法的数据库连接字符串 连接信息为" + url + "用户名" + username + "密码" + password + "异常信息", e);
				i++;
				if (i % 5 == 0) {
					return null;
				}
			} catch (ClassNotFoundException e) {
				logger.error("没有数据库驱动包" + e);
				return null;
			}
		}
		return connection;
	}
	public static Connection kerberosIdent(String urlStr)
	{
		Connection con=null;
		 try {

			  System.setProperty("java.security.krb5.conf","conf/krb5.conf");
			  org.apache.hadoop.conf.Configuration conf = new     org.apache.hadoop.conf.Configuration();
			  conf.set("java.security.krb5.conf","conf/krb5.conf");
			  conf.set("hadoop.security.authentication", "Kerberos");
		     // conf.set("keytab.file","C:/Users/gaoyu/Desktop/BOCO.keytab");
		      //conf.set("keytab.file","/config/BOCO.keytab");
			  String kerberos_count=ThreadPool.kerberos_count;
			  if(kerberos_count==null)
			  {
				  kerberos_count="boco/admin@HADOOP.COM";
			  }
		      UserGroupInformation.setConfiguration(conf);
		      UserGroupInformation.loginUserFromKeytab(kerberos_count, "conf/boco.keytab");      
		      System.out.println("getting connection");
		      con = DriverManager.getConnection(urlStr);
		     // Connection con = DriverManager.getConnection("jdbc:hive2://10.12.1.217:10000/default;principal=hive/cloud003@HADOOP.COM");
		 }
		 
		  catch (Exception e) {
			  Util.logger.info("认证kerberos异常"+e);
			  }
		 return con;
		 
	}
	public static void release(ResultSet rs, Statement st, Connection con) {

		try {
			if (rs != null) {
				logger.info("关闭rs");
				rs.close();
				logger.info("关闭rs完成");
				rs = null;
			}
			if (st != null) {
				logger.info("关闭st");
				st.close();
				logger.info("关闭st完成");
				st = null;
			}
			if (con != null) {
				logger.info("关闭con");
				con.close();
				logger.info("关闭con完成");
				con = null;
			}

		} catch (SQLException e) {
			logger.warn("无法正常关闭数据库链接", e);
		} finally {
			try {
				if (con != null) {
					con.close();
					con = null;
				}
			} catch (SQLException e) {
				logger.warn("无法正常关闭数据库连接" + e);
			}

		}

	}

	public static void release(PreparedStatement stmt, ResultSet rs, Statement st, Connection con) {

		try {
			if (stmt != null) {
				stmt.close();
				stmt = null;
			}
			if (rs != null) {
				rs.close();
				rs = null;
			}
			if (st != null) {
				st.close();
				st = null;
			}
			if (con != null) {
				con.close();
				con = null;
			}

		} catch (SQLException e) {
			logger.warn("无法正常关闭数据库链接", e);
		} finally {
			try {
				if (con != null) {
					con.close();
					con = null;
				}
			} catch (SQLException e) {
				logger.warn("无法正常关闭数据库连接" + e);
			}

		}

	}

	public static String mkfile(String path, String filename) {

		// path表示你所创建文件的路径
		// String path = "/tmp/";
		path = path.endsWith("/") ? path : path + "/";
		File f = new File(path);
		if (!f.exists()) {
			f.mkdirs();
		}
		File file = new File(f, filename);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return path + filename;

	}

	public static String mkfile(String filename) {

		// path表示你所创建文件的路径
		String path = "/tmp/";
		File f = new File(path);
		if (!f.exists()) {
			f.mkdirs();
		}
		File file = new File(f, filename);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return path + filename;

	}

	public static String getData() {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");// 设置日期格式
		return df.format(new Date());// new Date()为获取当前系统时间
	}

	public static void Finsh(String taskid, String info) {

		String date = getData();
		Connection con = null;
		PreparedStatement st = null;
		try {
			con = Util.getConnection(ThreadPool.URL, ThreadPool.USER, ThreadPool.PASSWD);
			while (con == null) {
				try {
					Thread.sleep(60000l);
				} catch (InterruptedException e) {
					// 不做处理
				}
				con = Util.getConnection(ThreadPool.URL, ThreadPool.USER, ThreadPool.PASSWD);
			}
			con.setAutoCommit(false);

			String upsql = "update sys_sharedata_task set state='执行完毕',result=?  ,finish_time=to_date('" + date
					+ "','YYYYMMDDHH24MISS') where task_id='" + taskid + "'";
			st = con.prepareStatement(upsql);
			st.setBlob(1, new ByteArrayInputStream(info.getBytes("GBK")));
			st.execute();
			con.commit();

		} catch (SQLException e) {
			logger.error(
					"无法将执行完毕信息更新到数据库 因为在更新时候发生SQL异常   应该更新的语句为 update sys_sharedata_task set state='执行完毕',result=?  ,finish_time=finish_time=to_date('"
							+ date + "','YYYYMMDDHH24MISS')  where task_id=" + taskid + "异常信息为",
					e);
		} catch (UnsupportedEncodingException e) {
			logger.error("出现不支持的编码格式", e);
		} finally {
			Util.release(null, st, con);
		}
	}

	public static void Start(String taskid) {
		String date = getData();
		String upsql = "update sys_sharedata_task set state='执行中',true_start_time=to_date('" + date
				+ "','YYYYMMDDHH24MISS') where task_id='" + taskid + "'";
		Connection con = Util.getConnection(ThreadPool.URL, ThreadPool.USER, ThreadPool.PASSWD);
		while (con == null) {
			try {
				Thread.sleep(60000l);
			} catch (InterruptedException e) {
				// 不做处理
			}
			con = Util.getConnection(ThreadPool.URL, ThreadPool.USER, ThreadPool.PASSWD);
		}
		try {
			con.setAutoCommit(false);
			Statement st = con.createStatement();
			st.executeUpdate(upsql);
			con.commit();

		} catch (SQLException e) {
			logger.warn("无法更新当前要处理的task任务状态 为执行中 SQL异常" + e.toString() + "SQL语句为" + upsql, e);
		} finally {
			Util.release(null, null, con);
		}

	}

	/**
	 * Description: 向FTP服务器上传文件
	 * 
	 * @Version1.0
	 * 
	 * @param url
	 *            FTP服务器hostname
	 * @param port
	 *            FTP服务器端口
	 * @param username
	 *            FTP登录账号
	 * @param password
	 *            FTP登录密码
	 * @param path
	 *            FTP服务器保存目录,如果是根目录则为“/”
	 * @param filename
	 *            上传到FTP服务器上的文件名
	 * @param input
	 *            本地文件输入流
	 * @return 成功返回true，否则返回false
	 */
	public static boolean uploadFile(String url, int port, String username, String password, String path,
			String filepath, String taskid, String fileEncoding) {
		FileInputStream in = null;
		try {
			in = new FileInputStream(new File(filepath));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		String filename = new File(filepath).getName();
		boolean result = false;
		FTPClient ftpClient = new FTPClient();
		try {
			ftpClient.connect(url, port);
			ftpClient.login(username, password);
			ftpClient.setControlEncoding(encoding);
			// 检验是否连接成功
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				logger.warn("无法连接到FTP服务器 请检查 " + taskid + "所关联任务的FTP服务器信息:URL=" + url + ",username:" + username
						+ "password:" + password);
				ftpClient.disconnect();
				return result;
			}
			// System.out.println("链接成功 准备上传");
			// 转移工作目录至指定目录下
			boolean change = ftpClient.changeWorkingDirectory(path);

			// ftpClient.enterLocalActiveMode();// 设置主动传输
			ftpClient.enterLocalPassiveMode();// 设置被动传输
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			if (change) {
				logger.info("切换ftp目录：" + path + "成功，开始上传");
				result = ftpClient.storeFile(new String(filename.getBytes(encoding), "iso-8859-1"), in);
				// if (result) {
				// System.out.println("上传成功!");
				// }
			} else {
				logger.info("切换ftp目录：" + path + "失败");
			}
			in.close();
			ftpClient.logout();
		} catch (UnsupportedEncodingException e) {
			logger.warn("在上传到FTP时候 指定了不正确的编码格式", e);
		} catch (IOException e) {
			logger.warn("无法正常上传文件到FTP服务器 有IO异常", e);
		} finally {
			if (ftpClient.isConnected()) {
				try {
					ftpClient.disconnect();
				} catch (IOException ioe) {
					logger.warn("无法正常关闭FTP客户端", ioe);
				}
			}
		}
		logger.info("上传完成：" + path + "成功");
		return result;
	}

	/**
	 * Description: 向FTP服务器上传文件
	 * 
	 * @Version1.0
	 * 
	 * @param url
	 *            FTP服务器hostname
	 * @param port
	 *            FTP服务器端口
	 * @param username
	 *            FTP登录账号
	 * @param password
	 *            FTP登录密码
	 * @param path
	 *            FTP服务器保存目录,如果是根目录则为“/”
	 * @param filename
	 *            上传到FTP服务器上的文件名
	 * @param input
	 *            本地文件输入流
	 * @return 成功返回true，否则返回false
	 */
	public static boolean uploadFileChangeNane(String url, int port, String username, String password, String path,
			String filepath, String taskid, String fileEncoding) {
		FileInputStream in = null;
		try {
			in = new FileInputStream(new File(filepath));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		String filename = new File(filepath).getName();
		boolean result = false;
		FTPClient ftpClient = new FTPClient();
		try {
			ftpClient.connect(url, port);
			ftpClient.login(username, password);
			ftpClient.setControlEncoding(encoding);
			// 检验是否连接成功
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				logger.warn("无法连接到FTP服务器 请检查 " + taskid + "所关联任务的FTP服务器信息:URL=" + url + ",username:" + username
						+ "password:" + password);
				ftpClient.disconnect();
				return result;
			}
			// System.out.println("链接成功 准备上传");
			// 转移工作目录至指定目录下
			boolean change = ftpClient.changeWorkingDirectory(path);
			// ftpClient.enterLocalActiveMode();// 设置主动传输
			ftpClient.enterLocalPassiveMode();// 设置被动传输
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			if (change) {
				logger.info("切换ftp目录：" + path + "成功，开始上传");
				result = ftpClient.storeFile(new String(filename.getBytes(encoding), "iso-8859-1"), in);
				ftpClient.rename(filename, filename.replace(".tmp", ".dat"));
				// if (result) {
				// System.out.println("上传成功!");
				// }
			} else {
				logger.info("切换ftp目录：" + path + "失败");
			}
			in.close();
			ftpClient.logout();
		} catch (UnsupportedEncodingException e) {
			logger.warn("在上传到FTP时候 指定了不正确的编码格式", e);
		} catch (IOException e) {
			logger.warn("无法正常上传文件到FTP服务器 有IO异常", e);
		} finally {
			if (ftpClient.isConnected()) {
				try {
					ftpClient.disconnect();
				} catch (IOException ioe) {
					logger.warn("无法正常关闭FTP客户端", ioe);
				}
			}
		}
		return result;
	}

	/**
	 * 此方法暂时不用
	 * 
	 * @param url
	 * @param port
	 * @param username
	 * @param password
	 * @param remotePath
	 * @param fileName
	 * @param localPath
	 * @return
	 */
	public static boolean downFile(String url, int port, String username, String password, String remotePath,
			String fileName, String localPath) {
		boolean result = false;
		FTPClient ftpClient = new FTPClient();
		try {
			ftpClient.setControlEncoding(encoding);

			ftpClient.connect(url, port);

			ftpClient.login(username, password);

			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE); // 设置文件传输类型为二进制

			int reply = ftpClient.getReplyCode(); // 获取ftp登录应答代码

			// 验证是否登陆成功
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient.disconnect();
				Util.logger.error(fileName + "--FTP server refused connection.");
				return result;
			}
			// 转移到FTP服务器目录至指定的目录下
			String linuxpath = new String(remotePath.getBytes(encoding), "iso-8859-1");
			ftpClient.changeWorkingDirectory(linuxpath);
			// 获取文件列表
			FTPFile[] fs = ftpClient.listFiles();
			for (FTPFile ff : fs) {
				if (ff.getName().equals(fileName)) {
					File localFile = new File(localPath + "/" + ff.getName());
					OutputStream is = new FileOutputStream(localFile);
					ftpClient.retrieveFile(ff.getName(), is);
					is.close();
				}
			}

			ftpClient.logout();
			result = true;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (ftpClient.isConnected()) {
				try {
					ftpClient.disconnect();
				} catch (IOException ioe) {
				}
			}
		}
		return result;
	}

	/**
	 * 
	 * @param endpoint
	 *            webServiceURL
	 * @param namespace
	 *            方法的命名空间
	 * @param method
	 *            方法名字
	 * @param orderid
	 *            订单ID
	 * @param code
	 *            应答码 例子为0 0成功 2失败
	 * @param ReplayDesc
	 *            应答描述成功或者失败
	 * @param ftpip
	 *            FTP的IP
	 * @param ftppoint
	 *            FTP的端口
	 * @param username
	 *            用户名
	 * @param password
	 *            密码
	 * @param charset
	 *            编码格式
	 * @param mode
	 *            直接传默认2
	 * @param filename
	 *            文件名字
	 * @param path
	 *            文件地址
	 * @param date
	 *            日期
	 * @param split
	 *            分割符
	 */
	public static void WebService(String endpoint, String namespace, String method, String orderid, String code,
			String ReplayDesc, String ftpip, String ftppoint, String username, String password, String charset,
			String mode, String filename, String path, String date, String split) {

		if (endpoint == null || "".equals(endpoint) || endpoint.length() == 0) {
			return;
		}

		try {
			// String endpoint = "http://10.0.7.222/sd/DataShare.asmx?wsdl";
			// 直接引用远程的wsdl文件
			// 以下都是套路
			// call.setOperationName(new
			// QName("http://tempuri.org/","PubSubDataDispatch"));
			// call.setOperationName("PubSubDataDispatch");//WSDL里面描述的接口名称
			// call.setUseSOAPAction(true);
			// call.setSOAPActionURI("http://tempuri.org/PubSubDataDispatch");
			// String temp = "140430153711";
			// String
			// temp="<?xml version='1.0'
			// encoding='UTF-8'?><Notifyservicecode='1891852519'><ReplyCode>0</ReplyCode><ReplyDesc>成功</ReplyDesc><FtpInfo><Ip>127.0.0.1</Ip><Port>21</Port><AuthName>user</AuthName><AuthPassword>123456</AuthPassword><FileNameCharset>UTF-8</FileNameCharset><Mode>2</Mode></FtpInfo><FileInfo><FileName>IPMS_001_20131011110000_20131011120000.csv</FileName><Path>ipms</Path><ReadyTime>20131011123000</ReadyTime><Separator>,</Separator></FileInfo></Notify>";
			// new Object[]{temp}
			Service service = new Service();
			Call call = (Call) service.createCall();
			call.setTargetEndpointAddress(new java.net.URL(endpoint));
			call.setOperationName(new QName(namespace, method));
			call.addParameter("param", org.apache.axis.encoding.XMLType.XSD_STRING, javax.xml.rpc.ParameterMode.IN);// 接口的参数
			call.setReturnType(org.apache.axis.encoding.XMLType.XSD_STRING);// 设置返回类型
			String temp = "<?xml version='1.0' encoding='UTF-8'?><Notifyservicecode='" + orderid + "'><ReplyCode>"
					+ code + "</ReplyCode><ReplyDesc>" + ReplayDesc + "</ReplyDesc><FtpInfo><Ip>" + ftpip
					+ "</Ip><Port>" + ftppoint + "</Port><AuthName>" + username + "</AuthName><AuthPassword>" + password
					+ "</AuthPassword><FileNameCharset>" + charset + "</FileNameCharset><Mode>" + mode
					+ "</Mode></FtpInfo><FileInfo><FileName>" + filename + "</FileName><Path>" + path
					+ "</Path><ReadyTime>" + date + "</ReadyTime><Separator>" + split
					+ "</Separator></FileInfo></Notify>";
			String result = (String) call.invoke(new Object[] { temp });
			// 给方法传递参数，并且调用方法
			logger.info("WEBSERVICE返回信息为" + result);
		} catch (ServiceException e) {
			logger.warn("webservice无法创建客户端代理", e);
		} catch (RemoteException e) {
			logger.warn("webservice无法调用远程接口方法，可能因为不能配置正确的SOAP协议", e);
		} catch (MalformedURLException e) {
			logger.warn("webservice无法创建客户端代理", e);
		}
	}

	public static StringBuffer GetTableHead(ResultSetMetaData data, String fileseparator) throws SQLException {

		StringBuffer table_head = new StringBuffer();
		table_head.setLength(0);
		for (int i = 1; i <= data.getColumnCount(); i++) {
			if (i < data.getColumnCount()) {
				table_head.append(data.getColumnLabel(i)).append(fileseparator);
			} else {
				table_head.append(data.getColumnLabel(i));
			}
		}
		return table_head.append("\n");
	}

	public static void WebService(String endpoint, String namespace, String method, String authCode, String filename,
			String filepath, String filesize, String testcount) {

		if (endpoint == null || "".equals(endpoint) || endpoint.length() == 0) {
			return;
		}

		try {
			Service service = new Service();
			Call call = (Call) service.createCall();
			call.setTargetEndpointAddress(new java.net.URL(endpoint));
			call.setSOAPActionURI("http://dss.boco/UploadNotifyToTS");
			call.setOperationName(new QName(namespace, method));
			call.addParameter("param", org.apache.axis.encoding.XMLType.XSD_STRING, javax.xml.rpc.ParameterMode.IN);// 接口的参数
			call.setReturnType(org.apache.axis.encoding.XMLType.XSD_STRING);// 设置返回类型
			String temp = "<?xml version=’1.0’ encoding=’utf-8’?>\n" + "<Root>\n" + "  <AuthCode>" + authCode
					+ "</AuthCode>\n" + "  <UploadFile>\n" + "    <FileName>" + filename + "</FileName>\n"
					+ "    <FilePath>" + filepath + "</FilePath>\n" + "    <FileSize>" + filesize + "</FileSize>\n"
					+ "    <TestCount>" + testcount + "</TestCount>\n" + "  </UploadFile>\n" + "<Root>\n";
			String result = (String) call.invoke(new Object[] { temp });
			// 给方法传递参数，并且调用方法
			logger.info("WEBSERVICE返回信息为" + result);
		} catch (ServiceException e) {
			logger.warn("webservice无法创建客户端代理", e);
		} catch (RemoteException e) {
			logger.warn("webservice无法调用远程接口方法，可能因为不能配置正确的SOAP协议", e);
		} catch (MalformedURLException e) {
			logger.warn("webservice无法创建客户端代理", e);
		}
	}

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {

		Util.uploadFile("10.25.176.81", 21, "pcap", "pcap",
				new String("/data/pcap/".getBytes("utf-8"), "iso-8859-1"), "D:/data/1.txt", "1111", "");

	}
}
