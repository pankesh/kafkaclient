package org.panksdmz.kafka.producer;

/**
 * CustomerEventsProducer class simulates the real time event from customer routers.
 *
 */
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class CustomerEventsProducer {

    private static final Logger LOG = Logger.getLogger(CustomerEventsProducer.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {

            System.out.println("Usage: CustomerEventsProducer <broker list> <zookeeper>");
            System.exit(-1);
        }

        LOG.debug("Using broker list:" + args[0] + ", zk conn:" + args[1]);

        Properties props = new Properties();
        props.put("metadata.broker.list", args[0]);
        props.put("zk.connect", args[1]);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        // props.put("partitioner.class", "org.panksdmz.kafka.producer.SimplePartitioner");
        props.put("request.required.acks", "1");

        String TOPIC = "customerevent2";
        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        Random random = new Random();

        String finalEvent = "";

        String eventInput = "router1.xml";
        String[] events = getRouterStates(eventInput);

        String[] customerIds = { "1", "2", "3", "4" };
        String[] routerName = { "router1", "router2", "router3", "router4" };
        String[] routerId = { "A123", "B234", "Z345", "X456" };

        int evtCnt = events.length;

        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < evtCnt; i++) {

                // Customer 1
                finalEvent = new Timestamp(new Date().getTime()) + "|" + customerIds[0] + "|" + routerId[0] + "|"
                        + events[random.nextInt(evtCnt)];
                try {
                    KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
                    LOG.info("Sending Messge #: " + routerName[0] + ": " + i + ", msg:" + finalEvent);
                    producer.send(data);
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Customer 2
                finalEvent = new Timestamp(new Date().getTime()) + "|" + customerIds[1] + "|" + routerId[1] + "|"
                        + events[random.nextInt(evtCnt)];
                try {
                    KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, "0", finalEvent);
                    LOG.info("Sending Messge #: " + routerName[1] + ": " + i + ", msg:" + finalEvent);
                    producer.send(data);
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Customer 3
                finalEvent = new Timestamp(new Date().getTime()) + "|" + customerIds[2] + "|" + routerId[2] + "|"
                        + events[random.nextInt(evtCnt)];
                try {
                    KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
                    LOG.info("Sending Messge #: " + routerName[2] + ": " + i + ", msg:" + finalEvent);
                    producer.send(data);
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Customer 4
                finalEvent = new Timestamp(new Date().getTime()) + "|" + customerIds[3] + "|" + routerId[3] + "|"
                        + events[random.nextInt(evtCnt)];
                try {
                    KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
                    LOG.info("Sending Messge #: " + routerName[3] + ": " + i + ", msg:" + finalEvent);
                    producer.send(data);
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        producer.close();
    }

    public static String[] getRouterStates(String fileName) throws Exception {
        String[] array = null;

        Document doc = null;

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        doc = db.parse(ClassLoader.getSystemResourceAsStream(fileName));
        doc.getDocumentElement().normalize();
        NodeList routerNode = doc.getElementsByTagName("Router");

        Node nNode = routerNode.item(0);
        if (nNode.getNodeType() == Node.ELEMENT_NODE) {
            Element eElement = (Element) nNode;
            String states = eElement.getElementsByTagName("states").item(0).getTextContent().toString();
            array = states.split(",");

        }

        return array;
    }
}
