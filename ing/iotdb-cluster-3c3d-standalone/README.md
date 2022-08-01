# 单机部署iotdb-cluster 3C3D
## 端口信息
### confignode:
1:22277,22278  
2:22377,22378  
3:22477,22478  

### datanode:
1:6667,9003,8777,40010,50010  
2:6767,9103,8877,40110,50110  
3:6867,9203,8977,40210,50210  


## 注意事项
!! 需要下载iotdb-0.14的distribution的all包，将lib拷贝到本项目的iotdb目录下


## 提供脚本
```
一键启动3c3d
start-all.sh
一键关闭3c3d
stop-all.sh
一键清理log、data
clear-all.sh
```