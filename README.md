# Flink Heap Consumer

This is a simple flink application that consumes the java heap space of Flink's Task Manager continuously. It was built in order to test an environment which scales depending on the java heap usage of Flink's Task Manager.

It periodically generates a large Java object which consumes the Java heap space. The heap usage can be confirmed in the Stdout of Task Manager through Flink Web UI.

# Prerequisite
1. Maven
2. Java 8
3. Apache Flink 1.6 

# Build
mvn clean package

# Run
```flink run flink-heap-consumer.jar --interval <interval to consume> --size <heap size to consume> --count <how many times to consume>```

You can see the heap usage of each Taskmanager by 

#### Examples for command line arguments 

Ex1: Below consumes 1 MB (1048576 bytes) every 2 seconds endlessly:

```--interval 2 --size 1048576 --count 0```

Ex2: Below consumes 1 KB (1024 bytes) every 5 seconds up to 100 times: 

```--interval 5 --size 1024 --count 100```