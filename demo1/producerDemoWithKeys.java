package newPackage.demo1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class producerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
       // System.out.println("hello world");
        //create producer properties
        Logger logger = LoggerFactory.getLogger(producerDemoWithKeys.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
       // create producer
        KafkaProducer<String, String>  producer= new KafkaProducer<String, String>(properties);
        // create record
        for(int i =0;i<10;i++) {
            String topic ="first_topic";
            String value = "hello world "+i;
            String key ="id_ "+i;
            // id_0 going to partition 1
            //id_1 going to partition 1
            // id_2 going to partition 1
            //id_3 going to partition 0
            // id_4 going to partition 0
            //id_5 going to partition 1
            // id_6 going to partition 1
            //id_7 going to partition 0
            // id_8 going to partition 0
            //id_9 going to partition 1


            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);
            logger.info("key: "+key);
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
            }).get(); //added to make it synchronous
        }
        // flush
        producer.flush();
        //close producer
        producer.close();
    }

}
