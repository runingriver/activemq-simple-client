package com.activemq.simple.client;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zongzhehu on 17-6-19.
 */
public class ConsumerTest {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerTest.class);
    private static final String DEV_BETA_URL = "tcp://127.0.0.1:56161?keepAlive=true";

    private static final int MESSAGE_COUNT = 50;

    public Producer createProducer(String queueName) {
        Producer producer = Producer.createDefault(queueName, DEV_BETA_URL);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String random = RandomStringUtils.random(10, true, true);
            producer.sendMessage(i + random);
        }
        return producer;
    }

    @Test
    public void receiveMessage() throws Exception {
        final AtomicInteger count = new AtomicInteger(1);
        Consumer consumer = Consumer.createDefault("test.queue.consumer", DEV_BETA_URL);
        consumer.receiveMessage(new MessageProcessor() {
            @Override
            public void processMessage(Message message) {
                if (message instanceof TextMessage) {
                    String text = getStringFromTextMessage(message);
                    logger.info("receive:{}:{}", count.getAndIncrement(), text);
                }
            }
        });

        Producer producer = createProducer("test.queue.consumer");

        TimeUnit.SECONDS.sleep(10);
        producer.close();
        consumer.close();
    }

    @Test
    public void custom() throws Exception {
        Consumer.ConsumerBuilder custom = Consumer.custom();
        custom.setServerUrl(DEV_BETA_URL).setDestinationName("test.queue.custom");
        custom.setOptimizedMessageDispatch(true)
                .setDispatchAsync(true)
                .setNonBlockingRedelivery(true)
                .setPrefetchSize(5)
                .setDeliveryMode(1)
                .setAcknowledge(1);
        Consumer consumer = custom.build();

        consumer.receiveMessage(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (message instanceof TextMessage) {
                    try {
                        String text = ((TextMessage) message).getText();
                        logger.info("receive:{}:{}", text);
                    } catch (JMSException e) {
                        logger.error("get message error.", e);
                    }
                }
            }
        });

        Producer producer = createProducer("test.queue.custom");

        TimeUnit.SECONDS.sleep(10);
        producer.close();
        consumer.close();
    }

    @Test
    public void createHeavyMessage() throws Exception {
        Consumer consumer = Consumer.createHeavyMessage("test.queue.heavy.message", DEV_BETA_URL, 1);
        consumer.receiveMessage(new MessageProcessor() {
            @Override
            public void processMessage(Message message) {
                if (message instanceof TextMessage) {
                    try {
                        String text = getStringFromTextMessage(message);
                        logger.info("receive:{}", text);
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        logger.error("sleep SECONDS error.", e);
                    }
                }
            }
        });

        Producer producer = createProducer("test.queue.heavy.message");

        TimeUnit.SECONDS.sleep(10);
        producer.close();
        consumer.close();
    }

}