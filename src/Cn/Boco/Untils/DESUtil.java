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
 * DES加密介绍 DES是一种对称加密算法，所谓对称加密算法即：加密和解密使用相同密钥的算法。DES加密算法出自IBM的研究，
 * 后来被美国政府正式采用，之后开始广泛流传，但是近些年使用越来越少，因为DES使用56位密钥，以现代计算能力，
 * 24小时内即可被破解。虽然如此，在某些简单应用中，我们还是可以使用DES加密算法，本文简单讲解DES的JAVA实现 。
 * 注意：DES加密和解密过程中，密钥长度都必须是8的倍数
 */
public class DESUtil {
	// 算法名称
	public static final String KEY_ALGORITHM = "DES";
	// 算法名称/加密模式/填充方式
	public static final String CIPHER_ALGORITHM = "DES/ECB/PKCS5Padding";// ,Zeros/PKCS5Padding/NoPadding

	// 测试
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
		// // 待加密内容
		// String str = "amigoxie";
		// // 密码，长度要是8的倍数
		// String password = "A1B2C3D4E5F60708";
		//
		// String result = DESUtil.encrypt(str, password);
		// System.out.println("加密后：" + result);
		//
		// // 直接将如上内容解密
		// try {
		// byte[] decryResult = DESUtil.decrypt(result, password);
		// System.out.println("解密后：" + new String(decryResult));
		// } catch (Exception e1) {
		// e1.printStackTrace();
		// }

	}

	/**
	 * 加密
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
			// 创建一个密匙工厂，然后用它把DESKeySpec转换成
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_ALGORITHM);
			SecretKey securekey = keyFactory.generateSecret(desKey);
			// Cipher对象实际完成加密操作
			Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
			// 用密匙初始化Cipher对象
			cipher.init(Cipher.ENCRYPT_MODE, securekey, random);
			// 正式执行加密操作
			return Base64.encode(cipher.doFinal(datasource));
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 文件加密并删除未加密文件
	 * 
	 * @param file
	 *            输入文件
	 * @param destFile
	 *            输出文件
	 * @param password
	 *            加密key
	 * @throws Exception
	 */
	public static void encryptFile(String file, String destFile, String password) throws Exception {

		SecureRandom random = new SecureRandom();
		DESKeySpec desKey = new DESKeySpec(password.getBytes());
		// 创建一个密匙工厂，然后用它把DESKeySpec转换成
		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_ALGORITHM);
		SecretKey securekey = keyFactory.generateSecret(desKey);
		// Cipher对象实际完成加密操作
		Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
		// 用密匙初始化Cipher对象
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
		// 删除加密前的文件
		FileUtils.deleteFile(file);
	}

	/**
	 * 解密
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
		// 创建一个密匙工厂，然后用它把DESKeySpec转换成
		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_ALGORITHM);
		SecretKey securekey = keyFactory.generateSecret(desKey);
		// Cipher对象实际完成加密操作
		Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
		// 用密匙初始化Cipher对象
		cipher.init(Cipher.DECRYPT_MODE, securekey, random);
		// 真正开始解密操作
		return cipher.doFinal(src);
	}

	/**
	 * 
	 * @param file
	 *            输入文件
	 * @param destFile
	 *            输出文件
	 * @param password
	 *            加密key
	 * @throws Exception
	 */
	public static void decryptFile(String file, String destFile, String password) throws Exception {

		SecureRandom random = new SecureRandom();
		DESKeySpec desKey = new DESKeySpec(password.getBytes());
		// 创建一个密匙工厂，然后用它把DESKeySpec转换成
		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_ALGORITHM);
		SecretKey securekey = keyFactory.generateSecret(desKey);
		// Cipher对象实际完成加密操作
		Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
		// 用密匙初始化Cipher对象
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
