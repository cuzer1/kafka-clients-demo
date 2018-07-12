package clients;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Properties properties = new Properties();

//		kafka bootstrap server config
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());

//		consumer config
		properties.setProperty("group.id", "test");
		properties.setProperty("enable.auto.commit", "true");
		properties.setProperty("auto.commit.interval.ms", "1000");

	
		KafkaConsumer<String, String> kafkaCon = new KafkaConsumer<String, String>(properties);
		kafkaCon.subscribe(Arrays.asList("second_topic"));
		
		while(true) {
			
			ConsumerRecords<String, String> consumerRecords = kafkaCon.poll(100);
			
			for(ConsumerRecord<String, String> consumerRecord :consumerRecords) {
				System.out.println("Partition= " + consumerRecord.partition() +
									", Offset= " + consumerRecord.offset() +
									", Key= " + consumerRecord.key() +
									", Value= " + consumerRecord.value()
									);
			}
		}
		

	}

}
