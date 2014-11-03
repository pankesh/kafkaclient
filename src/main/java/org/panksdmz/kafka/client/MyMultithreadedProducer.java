package org.panksdmz.kafka.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MyMultithreadedProducer {
	
	public static void main(String[] args) {
		ExecutorService fixPoolExecutor = Executors.newCachedThreadPool();
		
		List<MyKafkaProducer> jobs = new ArrayList<MyKafkaProducer>();
		jobs.add(new MyKafkaProducer());
		jobs.add(new MyKafkaProducer());
		jobs.add(new MyKafkaProducer());
		
		try {
			fixPoolExecutor.invokeAll(jobs);
		} catch (InterruptedException e) {
			System.out.println(e);
			e.printStackTrace();
		}
		
		
	}

}
