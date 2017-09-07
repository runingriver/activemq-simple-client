package com.activemq.simple.client;


import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BrokerMessageTest {
    private static final Logger logger = LoggerFactory.getLogger(BrokerMessageTest.class);
    private static final String DEV_BETA_URL = "tcp://127.0.0.1:56161?wireFormat.maxInactivityDuration=10000";

    /**
     * tcp下测试sock连接被回收.keepalive=false
     *
     * @throws Exception
     */
    @Test
    public void testInactiveMonitor() throws Exception {
        Producer producer = Producer.createDefault(DEV_BETA_URL, "test.inactive.monitor");
        for (int i = 1; i <= 20; i++) {
            String random = RandomStringUtils.random(5, true, true);
            producer.sendMessage(i + random);
        }

        Consumer consumer = Consumer.createDefault(DEV_BETA_URL, "test.inactive.monitor");
        consumer.receiveMessage(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (message instanceof TextMessage) {
                    try {
                        String text = ((TextMessage) message).getText();
                        logger.info("message:{}", text);
                    } catch (JMSException e) {
                        logger.error("exception.", e);
                    }
                }
            }
        });

        TimeUnit.SECONDS.sleep(600);

        logger.info("after a long time....");
        producer.sendMessage("test the connection is close or not.");

        TimeUnit.SECONDS.sleep(30);
        producer.close();
        consumer.close();
    }

    public static final String TEST_MQ_URL = "tcp://10.90.184.69:56161";

    /**
     * 测试session线程池,Transport线程池
     */
    @Test
    public void testMQThreadConsumer() throws JMSException {
        Consumer consumer = Consumer.createDefault("test.mq.thread", TEST_MQ_URL);
        final CountDownLatch latch = new CountDownLatch(1);
        consumer.receiveMessage(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (message instanceof TextMessage) {
                    try {
                        String text = ((TextMessage) message).getText();
                        logger.info("receive message:{}", text);
                        if (StringUtils.equals(text, "shutdown")) {
                            latch.countDown();
                        }
                    } catch (JMSException e) {
                        logger.error("exception.", e);
                    }
                }
            }
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("exception.", e);
        }
    }

    @Test
    public void testMQThreadProducer() throws InterruptedException {
        Producer producer = Producer.createDefault("test.mq.thread", TEST_MQ_URL);
        for (int i = 1; i <= 20000; i++) {
            String random = RandomStringUtils.random(50, true, true);
            producer.sendMessage(i + " " + random);
        }
        TimeUnit.SECONDS.sleep(120);

        producer.sendMessage("shutdown");
    }

}