package util;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

import static kafka.Config.*;

public class CreateKafkaProducer {

	public static KafkaProducer gerProducer() {
		Properties prop = new Properties();
		prop.put("bootstrap.servers", KAFKA_IP);
		prop.put("key.serializer", KEY_SERIALIZER);
		prop.put("value.serializer", VALU_ESERIALIZER);

		return new KafkaProducer(prop);
	}
}
