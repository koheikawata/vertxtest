package com.koheikawata;

import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.util.HashMap;
import java.util.Map;
import io.vertx.core.Vertx;

public class KafkaProducerDemo
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );

        Vertx vertx = Vertx.vertx();

        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "testkoheieventhubs.servicebus.windows.net:9093");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "1");

        // use producer for interacting with Apache Kafka
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config);

        for (int i = 0; i < 5; i++) {
            // only topic and message value are specified, round robin on destination partitions
            KafkaProducerRecord<String, String> record =
                KafkaProducerRecord.create("test", "message_" + i);

            producer.write(record);
        }
    }

}
