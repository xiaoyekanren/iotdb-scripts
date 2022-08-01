# coding=utf-8
from kafka import KafkaConsumer
# from kafka.structs import TopicPartition

# consumer.bootstrap.servers=192.168.35.119:9092,192.168.35.121:9092,192.168.35.122:9092
# consumer.zookeeper.ip=192.168.35.119:2181
# consumer.group.id=iotdb_save_kobelco_1
# consumer.max.poll.records=1
# consumer.enable.auto.commit=true
# consumer.auto.offset.reset=smallest
# #consumer.auto.offset.reset=latest
# consumer.auto.commit.interval.ms=1000
# consumer.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
# consumer.value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer
# #源码队列kafka消费者Topic
# consumer.topic=TYP_KTP_Kobelco_Decode,TYP_KTP_Kobelco_DecodeRsc
# enableCompression=false

consumer = KafkaConsumer(group_id='my-group2', auto_offset_reset='earliest', bootstrap_servers=['192.168.35.119:9092', '192.168.35.121:9092', '192.168.35.122:9092'])
consumer.subscribe(topics=['TYP_KTP_Kobelco_Decode', 'TYP_KTP_Kobelco_DecodeRsc'])


if __name__ == '__main__':
    # print(consumer.partitions_for_topic("TYP_KTP_Kobelco_Decode"))  # 获取xx主题的分区信息
    # print(consumer.topics())  # 获取主题列表
    # print(consumer.subscription())  # 获取当前消费者订阅的主题
    # print(consumer.assignment())  # 获取当前消费者topic、分区信息
    # print(consumer.beginning_offsets(consumer.assignment()))  # 获取当前消费者可消费的偏移量
    # consumer.seek(TopicPartition(topic=u'test', partition=0), 5)  # 重置偏移量，从第5个偏移量消费
    for message in consumer:
        print(message)
