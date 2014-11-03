package org.panksdmz.kafka.client;
 
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
 
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
 
public class MyKafkaConsumer {
    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;
 
    public MyKafkaConsumer(String zookeeperConnectionString, String aGroupId, String topicToConsumer) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(zookeeperConnectionString, aGroupId));
        this.topic = topicToConsumer;
    }
 
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }
 
    public void run(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(numThreads);
 
        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new TopicConsumer(stream, threadNumber));
            threadNumber++;
        }
    }
 
    private static ConsumerConfig createConsumerConfig(String zookeeperConnectionString, String aGroupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperConnectionString);
        props.put("group.id", aGroupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
 
        return new ConsumerConfig(props);
    }
 
    public static void main(String[] args) {
        String zooKeeper = "localhost:2181";
        String groupId = "group1";
        String topic = "my-replicated-topic";
        int threads = 4;
 
        MyKafkaConsumer example = new MyKafkaConsumer(zooKeeper, groupId, topic);
        example.run(threads);
 
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {
 
        }
        example.shutdown();
    }
}