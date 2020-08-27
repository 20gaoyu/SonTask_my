package Cn.Boco.Untils;

import java.io.File;
import java.io.FileInputStream;
//import java.io.FileNotFoundException;
import java.io.FileOutputStream;
//import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

public class FileUtils {

	public static Logger logger = Logger.getLogger(Util.class);

	/**
	 * �ļ�ѹ����gz��ʽ
	 * 
	 * @param inFileName
	 */
	public static String compressFile(String inFileName) {
		String outFileName = inFileName + ".gz";
		FileInputStream in = null;
		GZIPOutputStream out = null;
		try {
			in = new FileInputStream(new File(inFileName));
			// ZipOutputStream zipOut = new ZipOutputStream(new
			// FileOutputStream(zipFile));//ѹ����zip
			out = new GZIPOutputStream(new FileOutputStream(outFileName));
			int count;
			byte data[] = new byte[1024 * 1024];
			while ((count = in.read(data, 0, 1024 * 1024)) != -1) {
				out.write(data, 0, count);

			}

			in.close();
			logger.debug("Completing the GZIP file..." + outFileName);
			out.flush();
			out.close();
			// ѹ����ɺ�ɾ��Դ�ļ�
			File file = new File(inFileName);
			file.delete();
		} catch (Exception e) {
			logger.error(e);
		}

		return outFileName;
	}

	/**
	 * ɾ���ļ�
	 * 
	 * @param path
	 */
	public static void deleteFile(String path) {
		File file = new File(path);
		if (file.exists()) {
			file.delete();
		}
	}

	public static void renameFile(String oldname, String newname) {
		if (!oldname.equals(newname)) {// �µ��ļ�������ǰ�ļ�����ͬʱ,���б�Ҫ����������
			File oldfile = new File(oldname);
			File newfile = new File(newname);
			if (!oldfile.exists()) {
				return;// �������ļ�������
			}
			if (newfile.exists())// ���ڸ�Ŀ¼���Ѿ���һ���ļ������ļ�����ͬ��������������
				System.out.println(newname + "�Ѿ����ڣ�");
			else {
				oldfile.renameTo(newfile);
			}
		} else {
			System.out.println("���ļ����;��ļ�����ͬ...");
		}
	}

	public static void main(String[] args) {
		compressFile("E:/testdir/out.xml");
	}
}
