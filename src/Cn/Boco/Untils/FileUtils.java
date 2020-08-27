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
	 * 文件压缩成gz格式
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
			// FileOutputStream(zipFile));//压缩成zip
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
			// 压缩完成后删除源文件
			File file = new File(inFileName);
			file.delete();
		} catch (Exception e) {
			logger.error(e);
		}

		return outFileName;
	}

	/**
	 * 删除文件
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
		if (!oldname.equals(newname)) {// 新的文件名和以前文件名不同时,才有必要进行重命名
			File oldfile = new File(oldname);
			File newfile = new File(newname);
			if (!oldfile.exists()) {
				return;// 重命名文件不存在
			}
			if (newfile.exists())// 若在该目录下已经有一个文件和新文件名相同，则不允许重命名
				System.out.println(newname + "已经存在！");
			else {
				oldfile.renameTo(newfile);
			}
		} else {
			System.out.println("新文件名和旧文件名相同...");
		}
	}

	public static void main(String[] args) {
		compressFile("E:/testdir/out.xml");
	}
}
