package clients;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Properties properties = new Properties();

//		kafka bootstrap server config
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

//		producer config
		properties.setProperty("acks", "1");
		properties.setProperty("retries", "1");
		properties.setProperty("linger.ms", "1");

		Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(
				properties);

		for (int key = 0; key < 20; key++) {
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("second_topic",
					Integer.toString(key), "message that has key= " + Integer.toString(key));
			producer.send(producerRecord);
		}

		producer.close();

	}

}
