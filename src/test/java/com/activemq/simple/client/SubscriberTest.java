package com.activemq.simple.client;

import com.activemq.simple.client.config.MqConstant;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.jms.TopicSubscriber;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by zongzhehu on 17-6-22.
 */
public class SubscriberTest {
    private static final Logger logger = LoggerFactory.getLogger(SubscriberTest.class);
    public static final int NUMBER = 100;

    public void publishMessage(String queueName) {
        Publisher publisher = Publisher.createDefault(queueName, MqConstant.DEV_BETA_SERVER_URL);
        for (int i = 0; i < NUMBER; i++) {
            String random = i + " " + RandomStringUtils.random(5, true, true);
            publisher.publish(random);
        }
        publisher.publish("shutdown");
        publisher.close();
    }


    public String getAdvisoryString() {
        StringBuilder advisorySB = new StringBuilder();
        advisorySB.append("ActiveMQ.Advisory.MasterBroker").append(",");
        advisorySB.append("ActiveMQ.Advisory.Queue").append(",");
        advisorySB.append("ActiveMQ.Advisory.Topic").append(",");
        advisorySB.append("ActiveMQ.Advisory.Connection");
        return advisorySB.toString();
    }

    /**
     * 对于Advisory类型的Topic,不能用持久化订阅者
     * 个人认为关闭Advisory类型消息,如有需要可开启.
     *
     * @throws Exception
     */
    @Test
    public void AdvisoryTest() throws Exception {
        Subscriber subscriber = Subscriber.createDefault(getAdvisoryString(), MqConstant.DEV_BETA_SERVER_URL);
        subscriber.receiveMessage(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (message instanceof ActiveMQMessage) {
                    ActiveMQMessage activeMessage = (ActiveMQMessage) message;
                    logger.info("---Advisory message:{}", activeMessage.toString());
                }
            }
        });

        TimeUnit.SECONDS.sleep(200);
        subscriber.close();
    }

    /**
     * 非持久化订阅者,只会获取当前生产者发出的消息
     *
     * @throws Exception
     */
    @Test
    public void test2() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Subscriber subscriber = Subscriber.createDefault("test.topic.test1", MqConstant.DEV_BETA_SERVER_URL);
        subscriber.receiveMessage(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (message instanceof TextMessage) {
                    try {
                        String text = ((TextMessage) message).getText();
                        logger.info("get topic:{}", text);
                        if (StringUtils.equals(text, "shutdown")) {
                            latch.countDown();
                        }
                    } catch (JMSException e) {
                        logger.error("error");
                    }
                }
            }
        });

        publishMessage("test.topic.test1");
        latch.await();
        subscriber.close();
    }

    /**
     * 订阅所有消息,必须是持久化订阅者
     *
     * @throws Exception
     */
    @Test
    public void test3() throws Exception {
        //先生产消息,然后关闭连接
        publishMessage();

        //开启一个持久化订阅者,test是否收到之前发送的所有消息
        Subscriber subscriber = Subscriber.createDurable("test.topic.test3",
                MqConstant.DEV_BETA_SERVER_URL, "test durable", "durable111");
        subscriber.receiveMessage(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (message instanceof TextMessage) {
                    try {
                        String text = ((TextMessage) message).getText();
                        logger.info("topic:{}", text);
                    } catch (JMSException e) {
                        logger.error("error");
                    }
                }
            }
        });

        TimeUnit.SECONDS.sleep(10);
        subscriber.close();
    }

    public void publishMessage() throws Exception {
        //没有设置clientId
        Publisher publisher = Publisher.createDefault("test.topic.test3", MqConstant.DEV_BETA_SERVER_URL);
        for (int i = 0; i < NUMBER; i++) {
            String random = i + " " + RandomStringUtils.random(5, true, true);
            publisher.publish(random);
        }
        publisher.close();
    }

    @Test
    public void custom() throws Exception {
        Publisher publisher = Publisher.custom()
                .setClientId("publisher custom")
                .setServerUrl(MqConstant.DEV_BETA_SERVER_URL)
                .setDestinationName("test.topic.test3").build();

        for (int i = 0; i < NUMBER; i++) {
            String random = i + " " + RandomStringUtils.random(5, true, true);
            publisher.publish(random);
        }
        publisher.publish("shutdown");

        Subscriber subscriber = Subscriber.custom()
                .setClientId("durable test3")
                .setSubscribeName("custom22")
                .setServerUrl(MqConstant.DEV_BETA_SERVER_URL)
                .setDestinationName("test.topic.test3").build();

        TopicSubscriber topicSubscriber = subscriber.getSubscriber();
        while (true) {
            Message message = topicSubscriber.receive();
            if (message != null) {
                if (message instanceof TextMessage) {
                    String text = ((TextMessage) message).getText();
                    logger.info("subscribe msg:{}",text );
                    if (StringUtils.equals("shutdown",text)) {
                        break;
                    }
                }
            }
        }

        publisher.close();
        subscriber.close();
    }
}