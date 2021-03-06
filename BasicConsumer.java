import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class BasicConsumer {
    public static void main(String[] args) {
        assert (args.length > 0): " No topic specified";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Arrays.asList(
                        "mplkfk-stg1-01:9093",
                        "mplkfk-stg1-02:9093",
                        "mplkfk-stg2-01:9093",
                        "mplkfk-stg2-02:9093"
                ));

        //configure the following three settings for SSL Encryption
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/hdfs/app/GCS_ANA/eloqua_processing/scripts/kafka.client.truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "cisco123");

        //configure the following three settings for SSL Authentication
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/hdfs/app/GCS_ANA/eloqua_processing/scripts/kafka.client.keystore.jks");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "cisco123");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "cisco123");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dce-smb-kafka--DSX9");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        TestConsumerRebalanceListener rebalanceListener = new TestConsumerRebalanceListener();

        //String for today's date to be set as the output filename
        DateFormat dateFormat = new SimpleDateFormat("yyMMdd");
        Date date = new Date();
        String todayDate = dateFormat.format(date);
        String fileName = todayDate + ".txt";
        String targetDataFile;
        String topic;

        //if-else case to catch different topics specified in command line
        //To add new topic, add another elif case with the requisite topic and targetDataFile
        if (args[0].equals("mplOrderData")){
            try{
                topic = "mplOrderData";
                targetDataFile = "/hdfs/app/GCS_ANA/gvscsde/projects/order_data/";

                consumer.subscribe(Collections.singletonList(topic), rebalanceListener);
                PrintWriter out = new PrintWriter(new FileWriter(targetDataFile + fileName), true);
                while (true){
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
                    for (ConsumerRecord<byte[], byte[]> record : records){
                        //System.out.println(record);

                        //Writes content of records to jar file in format of the string below
                        String str = record.topic() + "|" + record.partition() + "|" + record.offset() + "|"
                                + record.key() + "|" + record.value();
                        out.println(str);

                        //Prints out records that are saved to .txt file
                        System.out.printf("Received Message topic =%s|partition =%s|offset = %d|key =%s|value =%s\n",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                    consumer.commitSync();
                }
            } catch (IOException e){
                e.printStackTrace();
            }
        } else if (args[0].equals("mplCartData")) {
            try {
                topic = "mplCartData";
                targetDataFile = "/hdfs/app/GCS_ANA/gvscsde/projects/cart_data/";

                //Subscribe to new topic
                consumer.subscribe(Collections.singletonList(topic), rebalanceListener);
                PrintWriter out = new PrintWriter(new FileWriter(targetDataFile + fileName), true);
                while (true){
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
                    for (ConsumerRecord<byte[], byte[]> record : records){
                        //System.out.println(record);

                        //Writes content of records to jar file in format of the string below
                        String str = record.topic() + "|" + record.partition() + "|" + record.offset() + "|"
                                + record.key() + "|" + record.value();
                        out.println(str);

                        //Prints out records that are saved to .txt file
                        System.out.printf("Received Message topic =%s|partition =%s|offset = %d|key =%s|value =%s\n",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                    consumer.commitSync();
                }

            } catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    private static class TestConsumerRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("Called onPartitionsRevoked with partitions:" + partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("Called onPartitionsAssigned with partitions:" + partitions);
        }
    }
}