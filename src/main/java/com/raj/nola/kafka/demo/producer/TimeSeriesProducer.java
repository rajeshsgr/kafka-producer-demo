package com.raj.nola.kafka.demo.producer;
import com.raj.nola.kafka.demo.config.SystemConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;


import java.util.Properties;
import java.util.Random;

public class TimeSeriesProducer {


    public static void main(String[] args) {

        System.out.println("Initializing Producer...");

        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, SystemConfig.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SystemConfig.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,"0");

        KafkaProducer<Long, Integer> producer = new KafkaProducer<Long, Integer>(props);

        Random random = new Random();

        System.out.println("Start sending messages...");

        for (int i = 1; i <= SystemConfig.maxEventCount; i++) {

            int randomNumber = random.nextInt(100);

            try {

                ProducerRecord<Long, Integer> record= new ProducerRecord<Long, Integer>(SystemConfig.topicName, System.nanoTime(),randomNumber);

                RecordMetadata recordMetadata = producer.send(record).get();

                String message = String.format("sent message to topic:%s partition:%s  offset:%s",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                System.out.println(message);

            }catch(Exception e){
                e.printStackTrace();
            }

        }

        System.out.println("Finished - Closing Kafka Producer.");
        producer.close();

    }
}
