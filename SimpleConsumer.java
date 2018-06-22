import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SimpleConsumer {
    public static void main(String[] args) throws Exception {
      /*if(args.length == 0){
         System.out.println("Enter topic name");
         return;
      }*/
        //Kafka consumer configuration settings
        KafkaConsumer<String, String> consumer = null;
        try {     String topicName = "mplCartData";
            Properties props = new Properties();

            props.put("bootstrap.servers", "mplkfk-prd1-01:9093,mplkfk-prd1-02:9093,mplkfk-prd2-01:9093,mplkfk-prd2-02:9093");

            props.put("group.id", "test123");

            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("security.protocol","SSL");
            props.put("ssl.truststore.password","C!sco123");
            props.put("ssl.keystore.password","C!sco123");
            props.put("ssl.key.password","C!sco123");

            props.put("ssl.truststore.location", "C:/Users/calprice/Documents/kafka.client.truststore.jks");
            props.put("ssl.keystore.location", "C:/Users/calprice/Documents/kafka.client.keystore.jks");

            consumer = new KafkaConsumer
                    <String, String>(props);

            //Kafka Consumer subscribes list of topics here.
            consumer.subscribe(Arrays.asList(topicName));

            //print the topic name
            System.out.println("Subscribed to topic " + topicName);

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records)

                    // print the offset,key and value for the consumer records.
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());
                // consumer.commitSync();
            }
        }
        finally {
            consumer.close();
        }
    }
}
