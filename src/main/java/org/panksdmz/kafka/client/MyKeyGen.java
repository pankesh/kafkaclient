package org.panksdmz.kafka.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MyKeyGen implements Callable<Integer> {

	private static int SEQUENCE = 0;
	private static String LOCK = "lock";
	
	public int getNextId() {
		synchronized (LOCK) {
			return SEQUENCE++;
		}
	}
	
	public static void main(String[] args) {
		ExecutorService fixPoolExecutor = Executors.newFixedThreadPool(2);
		
		List<MyKeyGen> jobs = new ArrayList<MyKeyGen>();
		jobs.add(new MyKeyGen());
		jobs.add(new MyKeyGen());
		jobs.add(new MyKeyGen());
		jobs.add(new MyKeyGen());
		jobs.add(new MyKeyGen());
		jobs.add(new MyKeyGen());
		jobs.add(new MyKeyGen());
		
		try {
			List<Future<Integer>> invokeAll = fixPoolExecutor.invokeAll(jobs);
			
//			for (Future<Integer> future : invokeAll) {
//			}
			
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
		
		
	}

	public Integer call() throws Exception {
		synchronized (LOCK) {
			System.out.println("Return value is " + ++SEQUENCE);
			return SEQUENCE;
		}
	}
}
