# kafka-samples
Apache Kafka samples 
1. [ Start. ](#start)
2. [ Topic. ](#topic)
3. [ Producer. ](#producer)
4. [ Consumer. ](#consumer)
5. [ Offsets. ](#offsets)

<a name="start"></a>
## Start
Start Zookeeper:  
```cmd
c:\kafka_2.12-2.4.0>zookeeper-server-start.bat ./config/zookeeper.properties
```
Start Kafka broker:  
```cmd
c:\kafka_2.12-2.4.0>kafka-server-start.bat ./config/server.properties
```

<a name="topic"></a>
## Topic
```cmd
kafka-topics --zookeeper 127.0.0.1:2181 --topic first_topic --create --partitions 3 --replication-factor 1
```
```cmd
kafka-topics --zookeeper 127.0.0.1:2181 --list
```
```cmd
kafka-topics --zookeeper 127.0.0.1:2181 --topic first_topic --describe
```
Output: 
 
Topic: first_topic      PartitionCount: 3       ReplicationFactor: 1    Configs:  
      Topic: first_topic      Partition: 0    Leader: 0       Replicas: 0     Isr: 0  
      Topic: first_topic      Partition: 1    Leader: 0       Replicas: 0     Isr: 0  
      Topic: first_topic      Partition: 2    Leader: 0       Replicas: 0     Isr: 0  

```cmd
kafka-topics --zookeeper 127.0.0.1:2181 --topic second_topic --delete
```
Topic `second_topic` is marked for deletion.  
Note: This will have no impact if delete.topic.enable is not set to true.
```cmd
kafka-topics --zookeeper 127.0.0.1:2181 --list
```
Output:  
first_topic  
second_topic - marked for deletion  

<a name="producer"></a>
## Producer
```cmd
kafka-console-producer --version
```
Output:  
2.4.0 (Commit:77a89fcf8d7fa018)
```cmd
kafka-console-producer --broker-list 127.0.0.1:9092 --topic first_topic
```

<a name="consumer"></a>
## Consumer
```cmd
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic
```
```cmd
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my-first-application
```
```cmd
kafka-consumer-groups --bootstrap-server localhost:9092 --list
```
Output:  
my-first-application
```cmd
kafka-consumer-groups --bootstrap-server localhost:9092 --group my-first-application --describe
```
Output:  
Consumer group 'my-first-application' has no active members.

|GROUP                |TOPIC           |PARTITION  |CURRENT-OFFSET  |LOG-END-OFFSET  |LAG             |CONSUMER-ID     |HOST            |CLIENT-ID|
| ------------------- | -------------- | --------- | -------------- | -------------- | -------------- | -------------- | -------------- | ------- |
|my-first-application |first_topic     |0          |6               |6               |0               |-               |-               |-  |
|my-first-application |first_topic     |1          |5               |5               |0               |-               |-               |-  |
|my-first-application |first_topic     |2          |3               |3               |0               |-               |-               |-  |

<a name="offsets"></a>
## Offsets
```cmd
kafka-consumer-groups --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --shift-by -2 --execute --topic first_topic
```
Output:  

| GROUP                          | TOPIC                          | PARTITION  | NEW-OFFSET |  
| ------------------------------ | ------------------------------ | ---------- | ---------- |
| my-first-application           | first_topic                    | 0          | 4          |
| my-first-application           | first_topic                    | 2          | 1          |
| my-first-application           | first_topic                    | 1          | 3          |  

