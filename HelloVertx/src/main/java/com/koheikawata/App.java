package com.koheikawata;

import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;


import java.util.concurrent.ExecutionException;

import com.koheikawata.IKafkaConstants;
import com.koheikawata.ProducerCreator;

public class App 
{
    public static void main( String[] args )
    {
        runProducer();
    }

    static void runProducer() {
        KafkaProducer<String, String> producer = ProducerCreator.createProducer();
        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
//            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, "This is record " + index);
            KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(IKafkaConstants.TOPIC_NAME, "This is record " + index);

            try {
            RecordMetadata metadata = producer.write(record).get();
                        System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
                 } 
            catch (ExecutionException e) {
                     System.out.println("Error in sending record");
                     System.out.println(e);
                  } 
             catch (InterruptedException e) {
                      System.out.println("Error in sending record");
                      System.out.println(e);
                  }
         }
    }
}
