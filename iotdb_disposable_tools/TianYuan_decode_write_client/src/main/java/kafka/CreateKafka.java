package kafka;

import cn.hutool.core.convert.Convert;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static run.Launcher.p;

public class CreateKafka {

	private ConsumerConnector consumer;

	public CreateKafka() {
		Properties props = new Properties();

		/**
		 * Zookeeper configuration
		 */
		props.put("auto.offset.reset", p.getProperty("consumer.auto.offset.reset"));
		props.put("zookeeper.connect", p.getProperty("consumer.zookeeper.ip"));
		props.put("group.id", p.getProperty("consumer.group.id"));
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("rebalance.max.retries", "5");
		props.put("rebalance.backoff.ms", "1200");
		props.put("auto.commit.interval.ms", "1000");

		ConsumerConfig config = new ConsumerConfig(props);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
	}

	public void consume() {

		Map<String, Integer> topicCountMap = new HashMap<>();
		// topicCountMap.put(Config.TOPIC, Config.KAFKA_CONSUMER_NUMBER);

		String[] topics = p.getProperty("consumer.topic").trim().split(",");
		for (String topic : topics) {
			topicCountMap.put(topic, 3);
		}

		String countStr = p.getProperty("iotdb.storageGroupCount");

		int storageGroupCount = Convert.toInt(countStr);

		ExecutorService executor = Executors.newFixedThreadPool(Config.KAFKA_CONSUMER_NUMBER * topics.length);

		/**
		 * Specify data decoder
		 */
		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

		// List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(Config.TOPIC);
		//
		// // for (final KafkaStream<byte[], byte[]> stream : streams) {
		// // executor.submit(new KafkaConsumerThread(stream));
		// // }
		//
		//
		// for (final KafkaStream<byte[], byte[]> stream : streams) {
		// executor.submit(new KafkaConsumerThread(stream));
		// }

		for (String topic : topics) {
			List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
			for (final KafkaStream<byte[], byte[]> stream : streams) {
				executor.submit(new KafkaConsumerThread(stream,storageGroupCount));
			}
		}
	}

}
