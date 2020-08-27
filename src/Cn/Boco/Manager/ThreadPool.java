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
			logger.warn("未找到资源文件 不能读取数据库IP ", e);
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
		logger.info("加载参数："+"if_kerberos:"+if_kerberos+" kerberos_count :"+kerberos_count
				+" if_ha:"+if_ha+" namenode_main:"+namenode_main+" namenode_bak:"+namenode_bak
	    		);
	}
	public static long _next_task_id = 0;
	public static final int DEFAULT_THREAD_NUM = 30;// 默认最大值
	public static final int MAX_THREAD_NUM = 10;// 现在最大值
	public int _cur_thread_num = 0;// 当前可用线程数目
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

	private ThreadPool(int thread_num) {// 实例一定的线程
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
		_is_closed = false;// 表示线程池不可用
		for (int i = 0; i < _cur_thread_num; ++i) {
			threads[i].start();
		}
	}

	public void close() {
		if (!_is_closed) {
			waitforfinish();
			_is_closed = true;// 表示线程池可用
			taskQueue.clear();// 清空任务序列
		}
		logger.info("Thread pool close!");
	}

	public void waitforfinish() {
		synchronized (this) {
			_is_closed = true;
			notifyAll();// 丢失现在监视对象，交出CPU主权,唤醒其他监事本对象的线程
		}
		for (int i = 0; i < _cur_thread_num; ++i) {
			threads[i].stopThread();
			// logger.info(String.format("Thread [%d] stop!", i));
		}
	}

	public void addTask(Task new_task) {
		if (_is_closed) {
			logger.warn("主线程被关闭   无法添加任务");
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
		private boolean _is_running = true;// 表示是否可运行

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
						/* 任务队列为空，则等待有新任务加入从而被唤醒 */
						taskQueue.wait();
					} catch (InterruptedException ie) {
						logger.warn("在等待任务队列时候被强制结束等待状态", ie);
					}
				}

				/* 取出任务执行 */
				r = (Task) taskQueue.remove(0);
				return r;
			}
		}

		public void stopThread() {
			_is_running = false;
			try {
				this.join();// 等待this线程对象执行完毕
			} catch (InterruptedException ex) {
				logger.warn("此异常为允许异常 ：线程被强制结束 等待状态 ", ex);
			}
		}
	}
}
