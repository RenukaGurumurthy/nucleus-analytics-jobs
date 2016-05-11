package org.gooru.migration.jobs;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SampleConsumer {
	public static KafkaConsumer<String, String> consumer;
	
	public static void main(String args[]) {
		String topics = "QA-CONTENT-LOG-TEST";
		Properties props = new Properties();
		props.put("bootstrap.servers", "54.183.121.104:9092");
		props.put("group.id", "consumer-group");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("enable.auto.commit", true);
		props.put("session.timeout.ms", 10000);
		props.put("fetch.min.bytes", 50000);
		props.put("receive.buffer.bytes", 262144);
		props.put("max.partition.fetch.bytes", 2097152);
		props.put("auto.commit.interval.ms", 1000);

		consumer = new KafkaConsumer<>(props);
		try {
			consumer.subscribe(Arrays.asList(topics.split(",")));
			int timeouts = 0;
			while (true) {
				ConsumerRecords<String, String> records = null;
				try{
					System.out.print("topic subscribe....");
					records = consumer.poll(0);
				}catch(Exception e){
					e.printStackTrace();
				}
				if (records.count() == 0) {
					timeouts++;
				} else {
					System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
					timeouts = 0;
				}
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("topic: " + record.topic());
					switch (record.topic()) {
					case "QA-CONTENT-LOG":
						System.out.println(record.value());

					default:
						throw new IllegalStateException(
								"Shouldn't be possible to get message on topic " + record.topic());
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
