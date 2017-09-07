package com.activemq.simple.client.system.test1;

import com.activemq.simple.client.Consumer;
import com.activemq.simple.client.Producer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.util.concurrent.TimeUnit;

/**
 * 测试生产者消费者基本使用
 */
public class QueueTest {
    private static final Logger logger = LoggerFactory.getLogger(QueueTest.class);
    private static final String TEST_QUEUE_NAME = "client.mq.queue.test";

    @Test
    public void sendMessageTest() {
        Producer producer = Producer.createDefault(TEST_QUEUE_NAME);
        producer.sendMessage("hello ,im ok.");
        producer.close();
    }

    @Test
    public void receiveMessageTest() throws JMSException {
        Consumer consumer = Consumer.createDefault(TEST_QUEUE_NAME);
        consumer.receiveMessage(null);
        sleepSeconds(5);
        consumer.close();
    }

    public static void sleepSeconds(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            logger.error("exception", e);
        }
    }
}
