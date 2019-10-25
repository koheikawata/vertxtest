package com.koheikawata;

import io.vertx.core.Vertx;
import java.util.HashMap;
import java.util.Map;
import io.vertx.kafka.client.producer.KafkaProducer;

import com.koheikawata.IKafkaConstants;

public class ProducerCreator {
    public static KafkaProducer<String, String> createProducer() {
        Vertx vertx = Vertx.vertx();
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", IKafkaConstants.KAFKA_BROKERS);
//        config.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
        config.put("key.serializer", LongSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return KafkaProducer.create(vertx, config);
    }
}