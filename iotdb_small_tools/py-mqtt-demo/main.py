# coding=utf-8
import paho.mqtt.client as mqtt

# MQTT broker的地址和端口
broker_address = "172.20.31.16"
broker_port = 1883
iotdb_user = 'admin'
iotdb_password = '123456'
iotdb_topic = "iotdb"


# MQTT连接成功的回调函数
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        # 连接成功
        print('Connection Succeed!')
    else:
        print(f'Connect Error status {rc}')
    # print("Connected with result code " + str(rc))
    # 连接成功后订阅IoTDB发布的主题
    client.subscribe(iotdb_topic)


# MQTT接收消息的回调函数
def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))
    # 在这里处理接收到的数据
    print("Received data:", msg.payload)


def main():
    # 创建MQTT客户端
    client = mqtt.Client()

    # 设置MQTT连接的回调函数
    client.on_connect = on_connect
    client.on_message = on_message

    # 连接到MQTT broker
    client.username_pw_set(iotdb_user, iotdb_password)

    client.connect(broker_address, broker_port, 60)

    # 循环监听MQTT消息
    client.loop_forever()


if __name__ == '__main__':
    main()
