1. 运行spidersForStreaming.py获取数据

2. 运行pushDateToKafka.py上传数据

3. 执行Python脚本：python3 pushDateToKafka_c.py

4. 提交工程的jar文件到Spark集群中：bin/spark-submit --class com.vidots.DStreamCount --master yarn --deploy-mode cluster ~/spark-jars/Streaming-1.0-SNAPSHOT.jar

5. 启动Web工程：node app.js