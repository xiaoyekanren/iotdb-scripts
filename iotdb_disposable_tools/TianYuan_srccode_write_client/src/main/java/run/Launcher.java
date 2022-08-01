package run;

import kafka.CreateKafka;
import org.apache.iotdb.session.pool.SessionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class Launcher {
	private static Logger log = LoggerFactory.getLogger(Launcher.class);
	public static Properties p = new Properties();

	public static SessionPool sessionPool = null;

	private static Set<String> cache = new HashSet<>();

	public static void main(String[] args) {
		try {
			String path = "";
			try (InputStream in = Launcher.class.getClassLoader().getResourceAsStream("config.properties")) {
				p = new Properties();
				p.load(in);
				path = p.getProperty("path");
			}

			try (InputStream in = Launcher.class.getClassLoader().getResourceAsStream(path)) {
				p = new Properties();
				p.load(in);
			}

			sessionPool = new SessionPool(p.getProperty("iotdb.ip"), 6667, "root", "root", 150, false);

			String countStr = p.getProperty("iotdb.storageGroupCount");

			// int storageGroupCount = Convert.toInt(countStr);
            //
			// try {
			// 	// 异常IMEI组
			// 	sessionPool.setStorageGroup("root.raw.999999");
			// } catch (Exception e) {
			// 	e.printStackTrace();
			// }
            //
			// try {
			// 	// 正常组
			// 	for (int i = 0; i < storageGroupCount; i++) {
			// 		sessionPool.setStorageGroup("root.raw." + padLeft(String.valueOf(i), countStr.length(), '0'));
			// 	}
			// } catch (Exception e) {
			// 	e.printStackTrace();
			// }

			new CreateKafka().consume();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String padLeft(String src, int len, char ch) {
		int diff = len - src.length();
		if (diff <= 0) {
			return src;
		}

		char[] charr = new char[len];
		System.arraycopy(src.toCharArray(), 0, charr, diff, src.length());
		for (int i = 0; i < diff; i++) {
			charr[i] = ch;
		}
		return new String(charr);
	}
}
