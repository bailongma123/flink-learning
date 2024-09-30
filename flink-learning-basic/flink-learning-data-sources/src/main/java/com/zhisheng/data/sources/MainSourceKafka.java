package com.zhisheng.data.sources;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 利用 flink kafka 自带的 source 读取 kafka 里面的数据
 */
public class MainSourceKafka {
    public static final String KAFKA_TOPIC = "flink-kafka-source-test";
    public static final String KEY_DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String VALUE_DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringDeserializer";

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.174.171:9092");
        props.put("zookeeper.connect", "192.168.174.171:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", KEY_DESERIALIZER_CLASS);  //key 反序列化
        props.put("value.deserializer", VALUE_DESERIALIZER_CLASS);
//        props.put("auto.offset.reset", "latest"); //value 反序列化
        props.put("auto.offset.reset", "earliest"); //value 反序列化

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(
                KAFKA_TOPIC,  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props)).setParallelism(1);

        dataStreamSource.print(); //把从 kafka 读取到的数据打印在控制台

        env.execute("Flink add data source");
    }
}
