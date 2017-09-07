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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 对mq集群做性能测试,请勿测试服务中机器.
 * 压力测试: 单队列压测160w左右,failover,调整zkSessionTimeout=10s后单队列800w(8G大小数据)无failover.
 * 性能测试: 单队列平均 2-3ms/per,中间值:  ,百分比分布:
 */
public class QpsTest {
    private static final Logger logger = LoggerFactory.getLogger(QpsTest.class);
    public static final String QPS_QUEUE_NAME = "client.mq.qps.test";
    public static final String QPS_QUEUE_DLQ = "ActiveMQ.DLQ";

    private static final int TEST1 = 10000;  //5w
    private static final int TEST2 = 50000; //5w
    private static final int TEST3 = 100000; //10w
    private static final int TEST4 = 500000; //50w
    private static final int TEST5 = 1000000; //100w
    private static final int TEST6 = 2000000; //200w

    private AtomicLong producerCount = new AtomicLong(0);
    private AtomicLong consumerCount = new AtomicLong(0);

    @Test
    public void singleThreadProducerTest() {
        Producer producer = Producer.createDefault(QPS_QUEUE_NAME);
        Stopwatch started = Stopwatch.createStarted();
        for (int i = 0; i < TEST2; i++) {
            producer.sendMessage(produceMessage());
        }
        started.stop();
        double average = started.elapsed(TimeUnit.MILLISECONDS) / TEST2;
        logger.info("produce {} message cost:{},ave:{} ms/per",TEST2 ,started.toString(), average);
    }

    @Test
    public void singleThreadConsumerTest() {
        Consumer consumer = Consumer.createDefault(QPS_QUEUE_NAME);
        MessageConsumer queue = consumer.getConsumer();
        int i = 0;
        try {
            while (true) {
                Message message = queue.receive();
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    try {
                        logger.info("{} message:{}", i++, textMessage.getText());
                    } catch (JMSException e) {
                        logger.error("exception", e);
                    }
                } else {
                    logger.info("other message:{}", message.toString());
                }
            }
        } catch (JMSException e) {
            logger.error("exception", e);
        }

    }


    @Test
    public void multiThreadProducerTest() {

    }

    @Test
    public void multiThreadConsumerTest() {

    }

    private static String produceMessage() {
        String random = RandomStringUtils.random(10, true, true);
        return random;
    }
}
