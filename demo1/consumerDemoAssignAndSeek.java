package newPackage.demo1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class consumerDemoAssignAndSeek {
    public static void main(String[] args) {
        //System.out.println("Hello world");
        String bootstrap = "127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(consumerDemoAssignAndSeek.class.getName());

        String topic = "first_topic";

        Properties properties=new Properties();
        //create consumer configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String,String>(properties);
        //assign and seek are used to replay data or fetch a specific message
        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        long offsetToReadFrom = 5L;
        consumer.assign(Arrays.asList(partitionToReadFrom));
        //seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        int numberOfMessagesToRead =5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar =0;

        // poll for new data
        while(keepOnReading)
        {
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for( ConsumerRecord<String, String> record :records)
            {
                numberOfMessagesReadSoFar +=1;
                logger.info("Key:   " +record.key() +"Value  "+record.value());
                logger.info("Partition:  "+record.partition()+" ,Offset:  "+ record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead)
                {
                    keepOnReading = false;
                    break;
                }

            }

        }
    }
}
