package newPackage.demo1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class producerDemo {
    public static void main(String[] args) {
       // System.out.println("hello world");
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
       // create producer
        KafkaProducer<String, String>  producer= new KafkaProducer<String, String>(properties);
        // create record
        ProducerRecord<String,String> record = new ProducerRecord<String,String>("first_topic","hello world");
        // send data
        producer.send(record);
        // flush
        producer.flush();
        //close producer
        producer.close();
    }
}
