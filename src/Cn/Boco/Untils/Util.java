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
	// logger.error("û�����ݿ�������"+e);
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
				Util.logger.warn("���쳣Ϊ�����쳣���ڵȴ���ȡSQL���Ӷ���ʱ��ǿ�ƽ����ȴ�״̬ �ٴγ��Ի�ȡ����", e);
			} catch (SQLException e) {
				i++;
				Util.logger.warn(
						"�޷����ӵ����ݿ� SQL������ϢΪ" + url + "�û���" + username + "����" + password + "�쳣��Ϣ" + e.getMessage(), e);
				if (i % 10 == 0) {
					return null;
				}
			} catch (IllegalArgumentException e) {
				// ���쳣һ����implus�׳�
				Util.logger.warn(
						"�޷����ӵ����ݿ� SQL  ���ڷǷ������ݿ������ַ��� ������ϢΪ" + url + "�û���" + username + "����" + password + "�쳣��Ϣ", e);
				i++;
				if (i % 5 == 0) {
					return null;
				}
			} catch (ClassNotFoundException e) {
				logger.error("û�����ݿ�������" + e);
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
					Util.logger.info("��Ҫ��֤kerberos");
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
				Util.logger.warn("���쳣Ϊ�����쳣���ڵȴ���ȡSQL���Ӷ���ʱ��ǿ�ƽ����ȴ�״̬ �ٴγ��Ի�ȡ����", e);
			} catch (SQLException e) {
				i++;
				Util.logger.warn(
						"�޷����ӵ����ݿ� SQL������ϢΪ" + url + "�û���" + username + "����" + password + "�쳣��Ϣ" + e.getMessage(), e);
				if (i % 10 == 0) {
					return null;
				}
			} catch (IllegalArgumentException e) {
				// ���쳣һ����implus�׳�
				Util.logger.warn(
						"�޷����ӵ����ݿ� SQL  ���ڷǷ������ݿ������ַ��� ������ϢΪ" + url + "�û���" + username + "����" + password + "�쳣��Ϣ", e);
				i++;
				if (i % 5 == 0) {
					return null;
				}
			} catch (ClassNotFoundException e) {
				logger.error("û�����ݿ�������" + e);
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
			  Util.logger.info("��֤kerberos�쳣"+e);
			  }
		 return con;
		 
	}
	public static void release(ResultSet rs, Statement st, Connection con) {

		try {
			if (rs != null) {
				logger.info("�ر�rs");
				rs.close();
				logger.info("�ر�rs���");
				rs = null;
			}
			if (st != null) {
				logger.info("�ر�st");
				st.close();
				logger.info("�ر�st���");
				st = null;
			}
			if (con != null) {
				logger.info("�ر�con");
				con.close();
				logger.info("�ر�con���");
				con = null;
			}

		} catch (SQLException e) {
			logger.warn("�޷������ر����ݿ�����", e);
		} finally {
			try {
				if (con != null) {
					con.close();
					con = null;
				}
			} catch (SQLException e) {
				logger.warn("�޷������ر����ݿ�����" + e);
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
			logger.warn("�޷������ر����ݿ�����", e);
		} finally {
			try {
				if (con != null) {
					con.close();
					con = null;
				}
			} catch (SQLException e) {
				logger.warn("�޷������ر����ݿ�����" + e);
			}

		}

	}

	public static String mkfile(String path, String filename) {

		// path��ʾ���������ļ���·��
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

		// path��ʾ���������ļ���·��
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
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");// �������ڸ�ʽ
		return df.format(new Date());// new Date()Ϊ��ȡ��ǰϵͳʱ��
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
					// ��������
				}
				con = Util.getConnection(ThreadPool.URL, ThreadPool.USER, ThreadPool.PASSWD);
			}
			con.setAutoCommit(false);

			String upsql = "update sys_sharedata_task set state='ִ�����',result=?  ,finish_time=to_date('" + date
					+ "','YYYYMMDDHH24MISS') where task_id='" + taskid + "'";
			st = con.prepareStatement(upsql);
			st.setBlob(1, new ByteArrayInputStream(info.getBytes("GBK")));
			st.execute();
			con.commit();

		} catch (SQLException e) {
			logger.error(
					"�޷���ִ�������Ϣ���µ����ݿ� ��Ϊ�ڸ���ʱ����SQL�쳣   Ӧ�ø��µ����Ϊ update sys_sharedata_task set state='ִ�����',result=?  ,finish_time=finish_time=to_date('"
							+ date + "','YYYYMMDDHH24MISS')  where task_id=" + taskid + "�쳣��ϢΪ",
					e);
		} catch (UnsupportedEncodingException e) {
			logger.error("���ֲ�֧�ֵı����ʽ", e);
		} finally {
			Util.release(null, st, con);
		}
	}

	public static void Start(String taskid) {
		String date = getData();
		String upsql = "update sys_sharedata_task set state='ִ����',true_start_time=to_date('" + date
				+ "','YYYYMMDDHH24MISS') where task_id='" + taskid + "'";
		Connection con = Util.getConnection(ThreadPool.URL, ThreadPool.USER, ThreadPool.PASSWD);
		while (con == null) {
			try {
				Thread.sleep(60000l);
			} catch (InterruptedException e) {
				// ��������
			}
			con = Util.getConnection(ThreadPool.URL, ThreadPool.USER, ThreadPool.PASSWD);
		}
		try {
			con.setAutoCommit(false);
			Statement st = con.createStatement();
			st.executeUpdate(upsql);
			con.commit();

		} catch (SQLException e) {
			logger.warn("�޷����µ�ǰҪ�����task����״̬ Ϊִ���� SQL�쳣" + e.toString() + "SQL���Ϊ" + upsql, e);
		} finally {
			Util.release(null, null, con);
		}

	}

	/**
	 * Description: ��FTP�������ϴ��ļ�
	 * 
	 * @Version1.0
	 * 
	 * @param url
	 *            FTP������hostname
	 * @param port
	 *            FTP�������˿�
	 * @param username
	 *            FTP��¼�˺�
	 * @param password
	 *            FTP��¼����
	 * @param path
	 *            FTP����������Ŀ¼,����Ǹ�Ŀ¼��Ϊ��/��
	 * @param filename
	 *            �ϴ���FTP�������ϵ��ļ���
	 * @param input
	 *            �����ļ�������
	 * @return �ɹ�����true�����򷵻�false
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
			// �����Ƿ����ӳɹ�
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				logger.warn("�޷����ӵ�FTP������ ���� " + taskid + "�����������FTP��������Ϣ:URL=" + url + ",username:" + username
						+ "password:" + password);
				ftpClient.disconnect();
				return result;
			}
			// System.out.println("���ӳɹ� ׼���ϴ�");
			// ת�ƹ���Ŀ¼��ָ��Ŀ¼��
			boolean change = ftpClient.changeWorkingDirectory(path);

			// ftpClient.enterLocalActiveMode();// ������������
			ftpClient.enterLocalPassiveMode();// ���ñ�������
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			if (change) {
				logger.info("�л�ftpĿ¼��" + path + "�ɹ�����ʼ�ϴ�");
				result = ftpClient.storeFile(new String(filename.getBytes(encoding), "iso-8859-1"), in);
				// if (result) {
				// System.out.println("�ϴ��ɹ�!");
				// }
			} else {
				logger.info("�л�ftpĿ¼��" + path + "ʧ��");
			}
			in.close();
			ftpClient.logout();
		} catch (UnsupportedEncodingException e) {
			logger.warn("���ϴ���FTPʱ�� ָ���˲���ȷ�ı����ʽ", e);
		} catch (IOException e) {
			logger.warn("�޷������ϴ��ļ���FTP������ ��IO�쳣", e);
		} finally {
			if (ftpClient.isConnected()) {
				try {
					ftpClient.disconnect();
				} catch (IOException ioe) {
					logger.warn("�޷������ر�FTP�ͻ���", ioe);
				}
			}
		}
		logger.info("�ϴ���ɣ�" + path + "�ɹ�");
		return result;
	}

	/**
	 * Description: ��FTP�������ϴ��ļ�
	 * 
	 * @Version1.0
	 * 
	 * @param url
	 *            FTP������hostname
	 * @param port
	 *            FTP�������˿�
	 * @param username
	 *            FTP��¼�˺�
	 * @param password
	 *            FTP��¼����
	 * @param path
	 *            FTP����������Ŀ¼,����Ǹ�Ŀ¼��Ϊ��/��
	 * @param filename
	 *            �ϴ���FTP�������ϵ��ļ���
	 * @param input
	 *            �����ļ�������
	 * @return �ɹ�����true�����򷵻�false
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
			// �����Ƿ����ӳɹ�
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				logger.warn("�޷����ӵ�FTP������ ���� " + taskid + "�����������FTP��������Ϣ:URL=" + url + ",username:" + username
						+ "password:" + password);
				ftpClient.disconnect();
				return result;
			}
			// System.out.println("���ӳɹ� ׼���ϴ�");
			// ת�ƹ���Ŀ¼��ָ��Ŀ¼��
			boolean change = ftpClient.changeWorkingDirectory(path);
			// ftpClient.enterLocalActiveMode();// ������������
			ftpClient.enterLocalPassiveMode();// ���ñ�������
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			if (change) {
				logger.info("�л�ftpĿ¼��" + path + "�ɹ�����ʼ�ϴ�");
				result = ftpClient.storeFile(new String(filename.getBytes(encoding), "iso-8859-1"), in);
				ftpClient.rename(filename, filename.replace(".tmp", ".dat"));
				// if (result) {
				// System.out.println("�ϴ��ɹ�!");
				// }
			} else {
				logger.info("�л�ftpĿ¼��" + path + "ʧ��");
			}
			in.close();
			ftpClient.logout();
		} catch (UnsupportedEncodingException e) {
			logger.warn("���ϴ���FTPʱ�� ָ���˲���ȷ�ı����ʽ", e);
		} catch (IOException e) {
			logger.warn("�޷������ϴ��ļ���FTP������ ��IO�쳣", e);
		} finally {
			if (ftpClient.isConnected()) {
				try {
					ftpClient.disconnect();
				} catch (IOException ioe) {
					logger.warn("�޷������ر�FTP�ͻ���", ioe);
				}
			}
		}
		return result;
	}

	/**
	 * �˷�����ʱ����
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

			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE); // �����ļ���������Ϊ������

			int reply = ftpClient.getReplyCode(); // ��ȡftp��¼Ӧ�����

			// ��֤�Ƿ��½�ɹ�
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient.disconnect();
				Util.logger.error(fileName + "--FTP server refused connection.");
				return result;
			}
			// ת�Ƶ�FTP������Ŀ¼��ָ����Ŀ¼��
			String linuxpath = new String(remotePath.getBytes(encoding), "iso-8859-1");
			ftpClient.changeWorkingDirectory(linuxpath);
			// ��ȡ�ļ��б�
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
	 *            �����������ռ�
	 * @param method
	 *            ��������
	 * @param orderid
	 *            ����ID
	 * @param code
	 *            Ӧ���� ����Ϊ0 0�ɹ� 2ʧ��
	 * @param ReplayDesc
	 *            Ӧ�������ɹ�����ʧ��
	 * @param ftpip
	 *            FTP��IP
	 * @param ftppoint
	 *            FTP�Ķ˿�
	 * @param username
	 *            �û���
	 * @param password
	 *            ����
	 * @param charset
	 *            �����ʽ
	 * @param mode
	 *            ֱ�Ӵ�Ĭ��2
	 * @param filename
	 *            �ļ�����
	 * @param path
	 *            �ļ���ַ
	 * @param date
	 *            ����
	 * @param split
	 *            �ָ��
	 */
	public static void WebService(String endpoint, String namespace, String method, String orderid, String code,
			String ReplayDesc, String ftpip, String ftppoint, String username, String password, String charset,
			String mode, String filename, String path, String date, String split) {

		if (endpoint == null || "".equals(endpoint) || endpoint.length() == 0) {
			return;
		}

		try {
			// String endpoint = "http://10.0.7.222/sd/DataShare.asmx?wsdl";
			// ֱ������Զ�̵�wsdl�ļ�
			// ���¶�����·
			// call.setOperationName(new
			// QName("http://tempuri.org/","PubSubDataDispatch"));
			// call.setOperationName("PubSubDataDispatch");//WSDL���������Ľӿ�����
			// call.setUseSOAPAction(true);
			// call.setSOAPActionURI("http://tempuri.org/PubSubDataDispatch");
			// String temp = "140430153711";
			// String
			// temp="<?xml version='1.0'
			// encoding='UTF-8'?><Notifyservicecode='1891852519'><ReplyCode>0</ReplyCode><ReplyDesc>�ɹ�</ReplyDesc><FtpInfo><Ip>127.0.0.1</Ip><Port>21</Port><AuthName>user</AuthName><AuthPassword>123456</AuthPassword><FileNameCharset>UTF-8</FileNameCharset><Mode>2</Mode></FtpInfo><FileInfo><FileName>IPMS_001_20131011110000_20131011120000.csv</FileName><Path>ipms</Path><ReadyTime>20131011123000</ReadyTime><Separator>,</Separator></FileInfo></Notify>";
			// new Object[]{temp}
			Service service = new Service();
			Call call = (Call) service.createCall();
			call.setTargetEndpointAddress(new java.net.URL(endpoint));
			call.setOperationName(new QName(namespace, method));
			call.addParameter("param", org.apache.axis.encoding.XMLType.XSD_STRING, javax.xml.rpc.ParameterMode.IN);// �ӿڵĲ���
			call.setReturnType(org.apache.axis.encoding.XMLType.XSD_STRING);// ���÷�������
			String temp = "<?xml version='1.0' encoding='UTF-8'?><Notifyservicecode='" + orderid + "'><ReplyCode>"
					+ code + "</ReplyCode><ReplyDesc>" + ReplayDesc + "</ReplyDesc><FtpInfo><Ip>" + ftpip
					+ "</Ip><Port>" + ftppoint + "</Port><AuthName>" + username + "</AuthName><AuthPassword>" + password
					+ "</AuthPassword><FileNameCharset>" + charset + "</FileNameCharset><Mode>" + mode
					+ "</Mode></FtpInfo><FileInfo><FileName>" + filename + "</FileName><Path>" + path
					+ "</Path><ReadyTime>" + date + "</ReadyTime><Separator>" + split
					+ "</Separator></FileInfo></Notify>";
			String result = (String) call.invoke(new Object[] { temp });
			// ���������ݲ��������ҵ��÷���
			logger.info("WEBSERVICE������ϢΪ" + result);
		} catch (ServiceException e) {
			logger.warn("webservice�޷������ͻ��˴���", e);
		} catch (RemoteException e) {
			logger.warn("webservice�޷�����Զ�̽ӿڷ�����������Ϊ����������ȷ��SOAPЭ��", e);
		} catch (MalformedURLException e) {
			logger.warn("webservice�޷������ͻ��˴���", e);
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
			call.addParameter("param", org.apache.axis.encoding.XMLType.XSD_STRING, javax.xml.rpc.ParameterMode.IN);// �ӿڵĲ���
			call.setReturnType(org.apache.axis.encoding.XMLType.XSD_STRING);// ���÷�������
			String temp = "<?xml version=��1.0�� encoding=��utf-8��?>\n" + "<Root>\n" + "  <AuthCode>" + authCode
					+ "</AuthCode>\n" + "  <UploadFile>\n" + "    <FileName>" + filename + "</FileName>\n"
					+ "    <FilePath>" + filepath + "</FilePath>\n" + "    <FileSize>" + filesize + "</FileSize>\n"
					+ "    <TestCount>" + testcount + "</TestCount>\n" + "  </UploadFile>\n" + "<Root>\n";
			String result = (String) call.invoke(new Object[] { temp });
			// ���������ݲ��������ҵ��÷���
			logger.info("WEBSERVICE������ϢΪ" + result);
		} catch (ServiceException e) {
			logger.warn("webservice�޷������ͻ��˴���", e);
		} catch (RemoteException e) {
			logger.warn("webservice�޷�����Զ�̽ӿڷ�����������Ϊ����������ȷ��SOAPЭ��", e);
		} catch (MalformedURLException e) {
			logger.warn("webservice�޷������ͻ��˴���", e);
		}
	}

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {

		Util.uploadFile("10.25.176.81", 21, "pcap", "pcap",
				new String("/data/pcap/".getBytes("utf-8"), "iso-8859-1"), "D:/data/1.txt", "1111", "");

	}
}
