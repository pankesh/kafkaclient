package org.panksdmz.kafka.client;

import java.util.concurrent.Callable;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class TopicConsumer implements Callable<Object> {

	private int threadNumber;
	private KafkaStream<byte[], byte[]> stream;

	public TopicConsumer(KafkaStream<byte[], byte[]> stream, int threadNumber) {
		this.stream = stream;
		this.threadNumber = threadNumber;
	}

	public Object call() throws Exception {
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext())
			System.out.println("Thread " + threadNumber + ": "
					+ new String(it.next().message()));
		System.out.println("Shutting down Thread: " + threadNumber);
		return null;
	}
}
