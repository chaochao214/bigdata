package com.atguigu.consumer;

import com.atguigu.utils.PropertityUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Properties;

public class HBaseConsumer {

    public static void main(String[] args) throws IOException, ParseException {

        //获取kafka配置信息
        Properties properties = PropertityUtil.getPropertity();

        //创建kafka消费者并订阅主题
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(properties.getProperty("kafka.topics")));

        HBaseDAO hBaseDAO = new HBaseDAO();

        //循环拉取数据并打印
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());
                    hBaseDAO.put(consumerRecord.value());
                }
            }
        } finally {
            hBaseDAO.close();
        }
    }
}
