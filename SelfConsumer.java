import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class SelfConsumer {
    //Kafka Consumer configuration settings
    private final static String TOPIC = "mplCartData";
    private final static String BOOTSTRAP_SERVERS =
            "mplkfk-prd1-01:9093,mplkfk-prd1-02:9093,mplkfk-prd2-01:9093,mplkfk-prd2-02:9093";

    /**
     * Creates new consumer of Kafka Producer, specified through the specific
     * bootstrapped servers
     * @return a consumer that is subscribed to target producter
     */
    private static KafkaConsumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put("bootstrap servers",
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "DSXTest123");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        //SSL security protocols
        props.put("security.protocol","SSL");
        props.put("ssl.truststore.password","C!sco123");
        props.put("ssl.keystore.password","C!sco123");
        props.put("ssl.key.password","C!sco123");

        /**
         * Local directory
         * Originally was:
         * props.put("ssl.truststore.location", "C:/Users/ykucheka/Documents/kafka_prod_cert/kafka.client.truststore.prod.jks");
         * props.put("ssl.keystore.location", "C:/Users/ykucheka/Documents/kafka_prod_cert/kafka.client.keystore.prod.jks");
         * Not sure why my file is not saved as a "kafka.client.keystore.prod.jks"
         */
        props.put("ssl.truststore.location", "C:/Users/calprice/Documents/kafka.client.truststore.jks");
        props.put("ssl.keystore.location", "C:/Users/calprice/Documents/kafka.client.keystore.jks");

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }


    /**
     * Processes records held by the consumer and received from the producer
     * @throws InterruptedException
     * @return N/A
     */
    static void runConsumer() throws InterruptedException {
        final KafkaConsumer<String, String> consumer = createConsumer();

        final int giveUp = 100;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

    /**
     * Main method. Just calls runConsumer().
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        runConsumer();
    }
}
