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
        System.out.println( "Hello World! \" " );

        Vertx vertx = Vertx.vertx();
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "testkoheieventhubs.servicebus.windows.net:9093");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "1");
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.mechanism", "PLAIN");
        config.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://testkoheieventhubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=V7yKG8ubEYifLgJpan6we7DqK9Sse5RcBH5cPQtTuR4=\"");

        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config);

        for (int i = 0; i < 50; i++) {
            KafkaProducerRecord<String, String> record = KafkaProducerRecord.create("testkoheikafka", "message_" + i);
            producer.write(record);

            System.out.println( "message sent" + i );
        }

/*        producer.partitionsFor("testkoheikafka", ar -> {
            if (ar.succeeded()) {

                for (PartitionInfo partitionInfo : ar.result()) {
                    System.out.println(partitionInfo);
                }
            }
        });
*/
    }
}
