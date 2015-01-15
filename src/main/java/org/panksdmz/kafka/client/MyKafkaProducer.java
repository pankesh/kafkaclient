package org.panksdmz.kafka.client;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Callable;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MyKafkaProducer implements Callable<String> {

	public static void main(String[] args) {
		try {
			new MyKafkaProducer().call();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String call() throws Exception {
		//create 20 events
		long events = 20;
		//..randomly
		MyKeyGen rnd = new MyKeyGen();

		//Initialize properties 
		Properties kafkaProps = new Properties();
		kafkaProps.put("metadata.broker.list", "0:9092,1:9093");
		kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
//		kafkaProps.put("partitioner.class", "example.producer.SimplePartitioner");
		kafkaProps.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(kafkaProps);

		Producer<String, String> producer = new Producer<String, String>(config);

		for (long nEvents = 0; nEvents < events; nEvents++) {
			long runtime = new Date().getTime();
			String ip = "192.168.2." + rnd.getNextId();
			String msg = runtime + ",www.example.com," + ip;

			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					"my-replicated-topic", ip, msg);
			producer.send(data);
		}
		producer.close();
		return null;
	}

}
