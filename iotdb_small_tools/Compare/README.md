## Compare-2IoTDB-specify-timeseries.py
逐点对比，2个IoTDB之间指定时间序列的正确性

## Compare-2IoTDB-count-morerows.py
使用count对比2个iotdb的全部序列点数是否一致
1. 可以指定每次查询的时间序列数量，增加对比速度
2. 可以使用pyinstaller将程序打包exe单文件
3. pyinstaller==5.0.1
```shell
pyinstaller -F Compare-2IoTDB-count.spec
```