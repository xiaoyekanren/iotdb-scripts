package run;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUnit;
import kafka.CreateKafka;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import static cn.hutool.core.util.CharUtil.DOT;
import static kafka.Config.*;

public class Launcher {
	private static Logger log = LoggerFactory.getLogger(Launcher.class);
	public static Properties p = new Properties();

	public static SessionPool sessionPool = null;

	public static boolean iscci = false;
	public static boolean iscty = false;

	public static TimeZone TIME_ZONE = TimeZone.getTimeZone(ZoneId.from(ZoneOffset.ofHours(8)));

	public static int timeFilter = 1;

	private static SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");

	public static void main(String[] args) {
		try {

			String path = initAndGetRealPath();

			try (InputStream in = Launcher.class.getClassLoader().getResourceAsStream(path)) {
				p = new Properties();
				p.load(in);
			}

            TOPIC = p.getProperty("producer.topic");

			boolean enableCompression = Convert.toBool(p.getProperty("enableCompression"));
			sessionPool = new SessionPool(p.getProperty("iotdb.ip"), 6667, "root", "root", 150, enableCompression);

			String countStr = p.getProperty("iotdb.storageGroupCount");
			timeThreshold = Convert.toLong(p.getProperty("timeThreshold"), DateUnit.HOUR.getMillis() * 24 * 7);

            timeFilter = Convert.toInt(p.getProperty("timefilter"), 1);

            int storageGroupCount = Convert.toInt(countStr);
			try {
				for (int i = 0; i < storageGroupCount; i++) {
					sessionPool.setStorageGroup("root." + p.getProperty("iotdb.username") + ".trans" + DOT
							+ padLeft(String.valueOf(i), countStr.length(), '0'));
				}
			} catch (Exception e) {
				if (e instanceof StatementExecutionException) {
					StatementExecutionException see = (StatementExecutionException) e;
					if (see.getStatusCode() != 300) {
						System.out.println("now: " + LocalDateTime.now());
						see.printStackTrace();
						System.exit(0);
					}
				}
			}

			new CreateKafka().consume();
		} catch (Exception e) {
			System.out.print(formatter.format(new Date(System.currentTimeMillis())));
			e.printStackTrace();
		}
	}

	private static String initAndGetRealPath() throws IOException {
		String path;
		try (InputStream in = Launcher.class.getClassLoader().getResourceAsStream("config.properties")) {
			p = new Properties();
			p.load(in);
			path = p.getProperty("path");

			iscci = path.equals("config_ctycci.properties");
			iscty = path.equals("config_cty.properties");

			KAFKA_IP = p.getProperty("producerDaily.bootstrap.servers");
			KEY_SERIALIZER = p.getProperty("producerDaily.key.serializer");
			VALU_ESERIALIZER = p.getProperty("producerDaily.value.serializer");

		}
		return path;
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
