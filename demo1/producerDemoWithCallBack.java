package newPackage.demo1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producerDemoWithCallBack {
    public static void main(String[] args) {
       // System.out.println("hello world");
        //create producer properties
        Logger logger = LoggerFactory.getLogger(producerDemoWithCallBack.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
       // create producer
        KafkaProducer<String, String>  producer= new KafkaProducer<String, String>(properties);
        // create record
        for(int i =0;i<10;i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world"+i);
            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata: .\n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing :" + e);
                    }
                }
            });
        }
        // flush
        producer.flush();
        //close producer
        producer.close();
    }

}
