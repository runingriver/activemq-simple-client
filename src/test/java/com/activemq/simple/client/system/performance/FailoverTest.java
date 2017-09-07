package com.activemq.simple.client.system.performance;

import com.google.common.base.Stopwatch;
import com.activemq.simple.client.Consumer;
import com.activemq.simple.client.Producer;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 对故障转移,消息丢失情况测试
 * 测试:producer,consumer同时工作,测试是否有消息丢失.
 * 结果: single queue,restart master,无消息丢失(结果不考虑网络环境,测试次数,队列数量关系)
 */
public class FailoverTest {
    private static final Logger logger = LoggerFactory.getLogger(FailoverTest.class);
    private static final String QPS_QUEUE_NAME = "client.mq.failover.test";
    private static final int SEND_NUMBER = 10000;

    private AtomicLong producerCount = new AtomicLong(0);
    private AtomicLong consumerCount = new AtomicLong(0);


    @Test
    public void producerTextMessageTest() {
        Producer producer = Producer.createDefault(QPS_QUEUE_NAME);
        Stopwatch started = Stopwatch.createStarted();
        for (int i = 0; i < SEND_NUMBER; i++) {
            String message = producerCount.incrementAndGet() + "--" + produceMessage();
            producer.sendMessage(message);
        }
        started.stop();
    }

    @Test
    public void consumerTextMessageTest() {
        Consumer consumer = Consumer.createDefault(QPS_QUEUE_NAME);
        MessageConsumer queue = consumer.getConsumer();
        try {
            while (true) {
                Message message = queue.receive();
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    logger.info("{} message:{}", consumerCount.incrementAndGet(), textMessage.getText());
                } else {
                    logger.info("other message:{}", message.toString());
                }
            }
        } catch (JMSException e) {
            logger.error("exception", e);
        }
        logger.info("count:{}", consumerCount.get());
    }

    private static String produceMessage() {
        String random = RandomStringUtils.random(10, true, true);
        return random;
    }
}
