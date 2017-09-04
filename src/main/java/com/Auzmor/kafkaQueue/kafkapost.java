package com.Auzmor.kafkaQueue;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class kafkapost {
	public void sendMessage(String message) throws Exception {

			Properties props = new Properties();
			props.put("bootstrap.servers","localhost:9092");
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

			Producer<String, String> producer = new KafkaProducer<String, String>(props);
				producer.send(new ProducerRecord<String, String>("mydemo","Person",message));
				System.out.println("message posting has been completed successfully........");
			producer.close();
		}
	}

