package Cn.Boco.Manager;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import Cn.Boco.SonTask.Task;
import Cn.Boco.Untils.Util;

public class ThreadPool {
	public static String URL = "";
	public static String USER = "";
	public static String PASSWD = "";
	public static String DbEncouding="";
	public static String kerberos_count="";
	public static String if_kerberos="";
	public static String if_ha="";
	public static String namenode_main="";
	public static String namenode_bak="";

	public static Logger logger = Logger.getLogger(ThreadPool.class);
	static {
		Properties p = new Properties();
		try {
			FileInputStream in = new FileInputStream("conf/db.properties");
			p.load(in);
			in.close();
		} catch (IOException e) {
			logger.warn("δ�ҵ���Դ�ļ� ���ܶ�ȡ���ݿ�IP ", e);
		}
		//DRIVER = ""==(String) p.get("driver")?"oracle.jdbc.driver.OracleDriver":(String) p.get("driver");
		URL = ""==(String) p.get("url")?"jdbc:oracle:thin:@10.12.1.128:1521:obidb2":(String) p.get("url");
		USER = ""==(String) p.get("username")?"sysdss_dc":(String) p.get("username");
		PASSWD = ""==(String) p.get("password")?"sysdss_dc":(String) p.get("password");
		DbEncouding = "" == (String) p.get("DbEncouding") ? "GBK" : (String) p.get("DbEncouding");
		if_kerberos="" == (String) p.get("if_kerberos") ? "false" : (String) p.get("if_kerberos");
		kerberos_count= ""==(String) p.get("kerberos_count")?"boco/admin@HADOOP.COM":(String) p.get("kerberos_count");
		if_ha="" == (String) p.get("if_ha") ? "false" : (String) p.get("if_ha");
		namenode_main="" == (String) p.get("namenode_main") ? "hdfsNameServer:8020" : (String) p.get("namenode_main");
		namenode_bak="" == (String) p.get("namenode_bak") ? "hdfsNameServer:8020" : (String) p.get("namenode_bak");
		logger.info("���ز�����"+"if_kerberos:"+if_kerberos+" kerberos_count :"+kerberos_count
				+" if_ha:"+if_ha+" namenode_main:"+namenode_main+" namenode_bak:"+namenode_bak
	    		);
	}
	public static long _next_task_id = 0;
	public static final int DEFAULT_THREAD_NUM = 30;// Ĭ�����ֵ
	public static final int MAX_THREAD_NUM = 10;// �������ֵ
	public int _cur_thread_num = 0;// ��ǰ�����߳���Ŀ
	public boolean _is_closed = true;
	public List<Task> taskQueue = Collections
			.synchronizedList(new LinkedList<Task>());
	public WorkThread[] threads;
	private static ThreadPool _instance = null;

	private ThreadPool() {
		_cur_thread_num = DEFAULT_THREAD_NUM;
		threads = new WorkThread[_cur_thread_num];
		for (int i = 0; i < _cur_thread_num; ++i) {
			threads[i] = new WorkThread(i);
		}
	}

	private ThreadPool(int thread_num) {// ʵ��һ�����߳�
		_cur_thread_num = thread_num;
		threads = new WorkThread[_cur_thread_num];
		for (int i = 0; i < _cur_thread_num; ++i) {
			threads[i] = new WorkThread(i);
		}
	}

	public static ThreadPool getInstance() {
		if (_instance == null) {
			synchronized (ThreadPool.class) {
				if (_instance == null) {
					_instance = new ThreadPool();
				}
			}
		}
		return _instance;
	}

	public void start() {
		_is_closed = false;// ��ʾ�̳߳ز�����
		for (int i = 0; i < _cur_thread_num; ++i) {
			threads[i].start();
		}
	}

	public void close() {
		if (!_is_closed) {
			waitforfinish();
			_is_closed = true;// ��ʾ�̳߳ؿ���
			taskQueue.clear();// �����������
		}
		logger.info("Thread pool close!");
	}

	public void waitforfinish() {
		synchronized (this) {
			_is_closed = true;
			notifyAll();// ��ʧ���ڼ��Ӷ��󣬽���CPU��Ȩ,�����������±�������߳�
		}
		for (int i = 0; i < _cur_thread_num; ++i) {
			threads[i].stopThread();
			// logger.info(String.format("Thread [%d] stop!", i));
		}
	}

	public void addTask(Task new_task) {
		if (_is_closed) {
			logger.warn("���̱߳��ر�   �޷��������");
			throw new IllegalStateException();
		}
		synchronized (taskQueue) {
			if (new_task != null) {
				taskQueue.add(new_task);
				taskQueue.notifyAll();
			}
		}
	}

	public int getTaskCount() {
		return taskQueue.size();
	}

	public class WorkThread extends Thread {
		@SuppressWarnings("unused")
		private int _index;
		private boolean _is_running = true;// ��ʾ�Ƿ������

		public WorkThread(int index) {
			_index = index;
		}

		public void run() {
			while (_is_running) {
				Task t = getTask();
				if (t != null) {
					t.run();
				} else {
					return;
				}
			}
		}

		public Task getTask() {
			if (_is_closed)
				return null;
			Task r = null;
			synchronized (taskQueue) {
				while (taskQueue.isEmpty()) {
					try {
						/* �������Ϊ�գ���ȴ������������Ӷ������� */
						taskQueue.wait();
					} catch (InterruptedException ie) {
						logger.warn("�ڵȴ��������ʱ��ǿ�ƽ����ȴ�״̬", ie);
					}
				}

				/* ȡ������ִ�� */
				r = (Task) taskQueue.remove(0);
				return r;
			}
		}

		public void stopThread() {
			_is_running = false;
			try {
				this.join();// �ȴ�this�̶߳���ִ�����
			} catch (InterruptedException ex) {
				logger.warn("���쳣Ϊ�����쳣 ���̱߳�ǿ�ƽ��� �ȴ�״̬ ", ex);
			}
		}
	}
}
