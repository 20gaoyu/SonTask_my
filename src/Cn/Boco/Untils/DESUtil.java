package Cn.Boco.Untils;

//import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
//import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SecureRandom;
import javax.crypto.spec.DESKeySpec;

import org.apache.axis.encoding.Base64;

import javax.crypto.SecretKeyFactory;
import javax.crypto.SecretKey;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;

/**
 * DES���ܽ��� DES��һ�ֶԳƼ����㷨����ν�ԳƼ����㷨�������ܺͽ���ʹ����ͬ��Կ���㷨��DES�����㷨����IBM���о���
 * ����������������ʽ���ã�֮��ʼ�㷺���������ǽ�Щ��ʹ��Խ��Խ�٣���ΪDESʹ��56λ��Կ�����ִ�����������
 * 24Сʱ�ڼ��ɱ��ƽ⡣��Ȼ��ˣ���ĳЩ��Ӧ���У����ǻ��ǿ���ʹ��DES�����㷨�����ļ򵥽���DES��JAVAʵ�� ��
 * ע�⣺DES���ܺͽ��ܹ����У���Կ���ȶ�������8�ı���
 */
public class DESUtil {
	// �㷨����
	public static final String KEY_ALGORITHM = "DES";
	// �㷨����/����ģʽ/��䷽ʽ
	public static final String CIPHER_ALGORITHM = "DES/ECB/PKCS5Padding";// ,Zeros/PKCS5Padding/NoPadding

	// ����
	public static void main(String args[]) {
		String password = "A1B2C3D4E5F60708";

		try {
			DESUtil.encryptFile("E:/testdir/in.xml", "E:/testdir/out.xml", password);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			DESUtil.decryptFile("E:/testdir/out.xml", "E:/testdir/decript.xml", password);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// // ����������
		// String str = "amigoxie";
		// // ���룬����Ҫ��8�ı���
		// String password = "A1B2C3D4E5F60708";
		//
		// String result = DESUtil.encrypt(str, password);
		// System.out.println("���ܺ�" + result);
		//
		// // ֱ�ӽ��������ݽ���
		// try {
		// byte[] decryResult = DESUtil.decrypt(result, password);
		// System.out.println("���ܺ�" + new String(decryResult));
		// } catch (Exception e1) {
		// e1.printStackTrace();
		// }

	}

	/**
	 * ����
	 * 
	 * @param datasource
	 *            byte[]
	 * @param password
	 *            String
	 * @return byte[]
	 */
	public static String encrypt(String str, String password) {
		try {
			byte[] datasource = str.getBytes();
			SecureRandom random = new SecureRandom();
			DESKeySpec desKey = new DESKeySpec(password.getBytes());
			// ����һ���ܳ׹�����Ȼ��������DESKeySpecת����
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_ALGORITHM);
			SecretKey securekey = keyFactory.generateSecret(desKey);
			// Cipher����ʵ����ɼ��ܲ���
			Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
			// ���ܳ׳�ʼ��Cipher����
			cipher.init(Cipher.ENCRYPT_MODE, securekey, random);
			// ��ʽִ�м��ܲ���
			return Base64.encode(cipher.doFinal(datasource));
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * �ļ����ܲ�ɾ��δ�����ļ�
	 * 
	 * @param file
	 *            �����ļ�
	 * @param destFile
	 *            ����ļ�
	 * @param password
	 *            ����key
	 * @throws Exception
	 */
	public static void encryptFile(String file, String destFile, String password) throws Exception {

		SecureRandom random = new SecureRandom();
		DESKeySpec desKey = new DESKeySpec(password.getBytes());
		// ����һ���ܳ׹�����Ȼ��������DESKeySpecת����
		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_ALGORITHM);
		SecretKey securekey = keyFactory.generateSecret(desKey);
		// Cipher����ʵ����ɼ��ܲ���
		Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
		// ���ܳ׳�ʼ��Cipher����
		cipher.init(Cipher.ENCRYPT_MODE, securekey, random);

		InputStream is = new FileInputStream(file);
		OutputStream out = new FileOutputStream(destFile);

		// FileWriter out = new FileWriter(new File(destFile));

		CipherInputStream cis = new CipherInputStream(is, cipher);
		byte[] buffer = new byte[1024];
		int r;
		while ((r = cis.read(buffer)) > 0) {
			// while (cis.read(buffer) > 0) {
			// out.write(Base64.encode(buffer));
			// System.out.println(Base64.encode(buffer));
			out.write(buffer, 0, r);
		}
		cis.close();
		is.close();
		out.close();
		// ɾ������ǰ���ļ�
		FileUtils.deleteFile(file);
	}

	/**
	 * ����
	 * 
	 * @param src
	 *            byte[]
	 * @param password
	 *            String
	 * @return byte[]
	 * @throws Exception
	 */
	public static byte[] decrypt(String str, String password) throws Exception {
		byte[] src = Base64.decode(str);
		SecureRandom random = new SecureRandom();
		DESKeySpec desKey = new DESKeySpec(password.getBytes());
		// ����һ���ܳ׹�����Ȼ��������DESKeySpecת����
		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_ALGORITHM);
		SecretKey securekey = keyFactory.generateSecret(desKey);
		// Cipher����ʵ����ɼ��ܲ���
		Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
		// ���ܳ׳�ʼ��Cipher����
		cipher.init(Cipher.DECRYPT_MODE, securekey, random);
		// ������ʼ���ܲ���
		return cipher.doFinal(src);
	}

	/**
	 * 
	 * @param file
	 *            �����ļ�
	 * @param destFile
	 *            ����ļ�
	 * @param password
	 *            ����key
	 * @throws Exception
	 */
	public static void decryptFile(String file, String destFile, String password) throws Exception {

		SecureRandom random = new SecureRandom();
		DESKeySpec desKey = new DESKeySpec(password.getBytes());
		// ����һ���ܳ׹�����Ȼ��������DESKeySpecת����
		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_ALGORITHM);
		SecretKey securekey = keyFactory.generateSecret(desKey);
		// Cipher����ʵ����ɼ��ܲ���
		Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
		// ���ܳ׳�ʼ��Cipher����
		cipher.init(Cipher.DECRYPT_MODE, securekey, random);

		InputStream is = new FileInputStream(file);
		OutputStream out = new FileOutputStream(destFile);

		CipherInputStream cis = new CipherInputStream(is, cipher);
		byte[] buffer = new byte[1024];
		int r;
		while ((r = cis.read(buffer)) > 0) {
			out.write(buffer, 0, r);
		}
		cis.close();
		is.close();
		out.close();
	}
}
