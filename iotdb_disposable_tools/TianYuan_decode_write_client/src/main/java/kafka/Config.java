package kafka;

import cn.hutool.core.date.DateUnit;

public class Config {

	public static final int KAFKA_CONSUMER_NUMBER = 3;

	public static final int maxRowNumber = 500;
	public static long timeThreshold = DateUnit.HOUR.getMillis();

	public static String KAFKA_IP;
	public static String KEY_SERIALIZER;
	public static String VALU_ESERIALIZER;
	public static String TOPIC;
}
