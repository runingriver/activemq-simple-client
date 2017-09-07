package com.activemq.simple.client;

import com.activemq.simple.client.config.MqConstant;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by zongzhehu on 17-6-22.
 */
public class PublisherTest {
    private static final Logger logger = LoggerFactory.getLogger(PublisherTest.class);
    public static final int NUMBER = 100;

    public Subscriber subscribe(String queueName) throws JMSException {
        final Subscriber subscriber = Subscriber.createDurable(queueName, MqConstant.DEV_BETA_SERVER_URL
                , RandomStringUtils.random(5, true, true), RandomStringUtils.random(5, true, true));
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        Message message = subscriber.getSubscriber().receive();
                        if (message == null) {
                            continue;
                        }
                        if (message instanceof TextMessage) {
                            String text = ((TextMessage) message).getText();
                            logger.info("subscribe msg:{}", text);
                            if (StringUtils.equals("shutdown", text)) {
                                break;
                            }
                        }
                    }
                } catch (JMSException e) {
                    logger.error("error");
                }
            }
        });

        return subscriber;
    }

    @Test
    public void publish() throws Exception {
        Subscriber subscribe = subscribe("test.publish.test1");

        Publisher publisher = Publisher.createDefault("test.publish.test1", MqConstant.DEV_BETA_SERVER_URL);
        for (int i = 0; i < NUMBER; i++) {
            String random = i + " " + RandomStringUtils.random(5, true, true);
            publisher.publish(random);
        }
        publisher.publish("shutdown");
        subscribe.close();
        publisher.close();
    }

    @Test
    public void publish1() throws Exception {
        Publisher publisher = Publisher.custom().setClientId("publish test33")
                .setServerUrl(MqConstant.DEV_BETA_SERVER_URL)
                .setDestinationName("test.publish.test2").build();
        for (int i = 0; i < NUMBER * 10; i++) {
            String random = i + " " + RandomStringUtils.random(10, true, true);
            publisher.publish(random);
        }
        publisher.publish("shutdown");

        final CountDownLatch latch = new CountDownLatch(1);
        Subscriber subscriber = Subscriber.createDurable("test.publish.test2", MqConstant.DEV_BETA_SERVER_URL,
                "publish test3123", "test2 name");
        subscriber.receiveMessage(new MessageProcessor() {
            @Override
            public void processMessage(Message message) {
                String stringFromMessage = getStringFromTextMessage(message);
                logger.info(stringFromMessage);
                if (StringUtils.equals("shutdown", stringFromMessage)) {
                    latch.countDown();
                }
            }
        });

        latch.await();

        publisher.close();
        subscriber.close();
    }

    //-----------------------------------------------------------------------------------------------------------
    @Test
    public void pubMessage() throws Exception {
        Publisher publisher = Publisher.createDefault("test.publish.test3", MqConstant.DEV_BETA_SERVER_URL);

        for (int i = 0; i < NUMBER * 10; i++) {
            String random = i + " " + RandomStringUtils.random(10, true, true);
            publisher.publish(random);
            TimeUnit.SECONDS.sleep(1);
        }

        publisher.close();
    }

    //pubMessage和subMessage一起测试,多起重启subMessage,看消息是否有序到达
    @Test
    public void subMessage() throws Exception {
        Subscriber subscriber = Subscriber.createDurable("test.publish.test3", MqConstant.DEV_BETA_SERVER_URL,
                "123qwe", "qwe123");
        subscriber.receiveMessage(new MessageProcessor() {
            @Override
            public void processMessage(Message message) {
                String stringFromMessage = getStringFromTextMessage(message);
                logger.info(stringFromMessage);
            }
        });
        TimeUnit.SECONDS.sleep(5);
        subscriber.close();
    }
    //-----------------------------------------------------------------------------------------------------------
}