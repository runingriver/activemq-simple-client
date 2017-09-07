package com.activemq.simple.client;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.activemq.simple.client.config.MqHelper;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zongzhehu on 17-6-19.
 */
public class ProducerTest {
    private static final Logger logger = LoggerFactory.getLogger(ProducerTest.class);
    private static final String DEV_BETA_URL = "failover:(tcp://127.0.0.1:56161)";

    public Consumer createConsumer(String queueName) throws Exception {
        Consumer consumer = Consumer.createDefault(queueName, DEV_BETA_URL);
        consumer.receiveMessage(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (message instanceof TextMessage) {
                    try {
                        String text = ((TextMessage) message).getText();
                        logger.info("receive:{}", text);
                    } catch (JMSException e) {
                        logger.error("exception.", e);
                    }
                }
            }
        });
        return consumer;
    }


    @Test
    public void createDefault() throws Exception {
        Consumer consumer = createConsumer("test.create.default");
        Producer producer = Producer.createDefault("test.create.default", DEV_BETA_URL);
        for (int i = 0; i < 20; i++) {
            String random = RandomStringUtils.random(5, true, true);
            producer.sendMessage(i + random);
        }

        TimeUnit.MINUTES.sleep(1);
        producer.close();
        consumer.close();
    }

    /**
     * 测试,连接broker队列需要配置,否则用户密码无效
     *
     * @throws Exception
     */
    @Test
    public void createDefault1() throws Exception {
        Producer producer = Producer.createDefault("test.create.default2", DEV_BETA_URL);
        for (int i = 1; i <= 20; i++) {
            String random = RandomStringUtils.random(5, true, true);
            producer.sendMessage(i + random);
        }

        Consumer consumer = createConsumer("test.create.default2");

        TimeUnit.SECONDS.sleep(30);
        producer.close();
        consumer.close();
    }

    @Test
    public void customTransaction() throws Exception {
        Consumer consumer = createConsumer("test.producer.transaction");
        Producer producerWithNoTansaction = Producer.custom().setCopyMessageOnSend(false)
                .setProducerMessagePriority(2)
                .setDestinationName("test.producer.transaction")
                .setServerUrl(DEV_BETA_URL)
                .setDeliveryMode(1).build();

        Producer producer = Producer.custom().setCopyMessageOnSend(false)
                .setProducerMessagePriority(2)
                .setDestinationName("test.producer.transaction")
                .setServerUrl(DEV_BETA_URL)
                .setTransacted(true)
                .setDeliveryMode(1).build();

        producer.sendMessage("test transaction message1");
        producerWithNoTansaction.sendMessage("another producer send a message.");
        producer.sendMessage("test transaction message2");
        producer.commit();

        TimeUnit.SECONDS.sleep(20);
        producer.close();
        consumer.close();
    }

    @Test
    public void createThroughtput() throws Exception {
        Consumer consumer = createConsumer("test.producer.transaction");

        Producer producerWithNoTansaction = Producer.custom().setCopyMessageOnSend(false)
                .setProducerMessagePriority(2)
                .setDestinationName("test.producer.transaction")
                .setServerUrl(DEV_BETA_URL)
                .setDeliveryMode(1).build();

        Producer producer = Producer.custom().setCopyMessageOnSend(false)
                .setProducerMessagePriority(2)
                .setDestinationName("test.producer.transaction")
                .setServerUrl(DEV_BETA_URL)
                .setTransacted(true)
                .setDeliveryMode(1).build();

        producer.sendMessage("test transaction rollback message1");
        producerWithNoTansaction.sendMessage("another producer send a message. test rollback.");
        producer.sendMessage("test transaction rollback message2");
        producer.rollback();

        TimeUnit.SECONDS.sleep(10);
        producer.close();
        consumer.close();
    }

    @Test
    public void testSendMessage2() throws Exception {
        Consumer consumer = Consumer.createDefault("test.producer.byteMessage", DEV_BETA_URL);

        consumer.receiveMessage(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (message instanceof BytesMessage) {
                    try {
                        long bodyLength = ((BytesMessage) message).getBodyLength();
                        byte[] value = new byte[(int) bodyLength];
                        int i = ((BytesMessage) message).readBytes(value);
                        logger.info("receive:length:{},i:{},content:{}", bodyLength, i, new String(value));
                    } catch (JMSException e) {
                        logger.error("exception.", e);
                    }
                }
            }
        });


        Producer producer = Producer.custom()
                .setCompressionMessage(true)
                .setDestinationName("test.producer.byteMessage")
                .setServerUrl(DEV_BETA_URL).build();

        String message = "hello world.";
        byte[] bytes = message.getBytes("utf-8");
        producer.sendMessage(bytes);

        TimeUnit.SECONDS.sleep(10);
        producer.close();
        consumer.close();
    }

    @Test
    public void testSendMessage3() throws Exception {
        Consumer consumer = Consumer.createDefault("test.producer.mapMessage", DEV_BETA_URL);

        consumer.receiveMessage(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (message instanceof MapMessage) {
                    try {
                        Enumeration mapNames = ((MapMessage) message).getMapNames();
                        while (mapNames.hasMoreElements()) {
                            String key = (String) mapNames.nextElement();
                            logger.info("map mapNames key:{}", key);
                            Object object = ((MapMessage) message).getObject(key);
                            logger.info("map value:{}", object.toString());
                        }
                    } catch (JMSException e) {
                        logger.error("exception.", e);
                    }
                }
            }
        });

        Producer producer = Producer.custom()
                .setCompressionMessage(true)
                .setDestinationName("test.producer.mapMessage")
                .setServerUrl(DEV_BETA_URL).build();

        HashMap<String, Object> mapMessage = Maps.newHashMap();
        mapMessage.put("int", new Integer(800));
        mapMessage.put("bool", new Boolean(false));
        mapMessage.put("string", new String("hello map message"));
        producer.sendMessage(mapMessage);

        TimeUnit.SECONDS.sleep(10);
        producer.close();
        consumer.close();
    }

    /**
     * 结果:第一条消息没有收到,第二条收到
     *
     * @throws Exception
     */
    @Test
    public void testMessageLiveTime() throws Exception {

        Producer producer = Producer.custom()
                .setMessageLiveTime(1000)
                .setServerUrl(DEV_BETA_URL)
                .setDestinationName("test.message.liveTime")
                .build();

        producer.sendMessage("first message.");
        TimeUnit.SECONDS.sleep(3);
        Consumer consumer = createConsumer("test.message.liveTime");
        producer.sendMessage("second message.");

        TimeUnit.SECONDS.sleep(10);
        producer.close();
        consumer.close();
    }

    @Test
    public void testSameThreadName() throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(5, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "hello-thread");
                return thread;
            }
        });
        for (int i = 0; i < 20; i++) {
            service.execute(new Runnable() {
                @Override
                public void run() {
                    logger.info("hello world,Thread Name:{}", Thread.currentThread().getName());

                    try {
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException e) {
                        logger.error("exception", e);
                    }
                }
            });
        }
        TimeUnit.SECONDS.sleep(100);
    }

    private static AtomicInteger count = new AtomicInteger(0);

    /**
     * 测试broker down掉,抛异常,消息是否会有丢失!
     * failover无timeout时,无消息丢失，其他情况下会有消息丢失！
     */
    @Test
    public void testBrokerDown() throws Exception {
        //mock消费者在不停地消费
        Consumer consumer = Consumer.createDefault("test.producer.exception", DEV_BETA_URL);
        final CountDownLatch latch = new CountDownLatch(1);
        consumer.receiveMessage(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (message instanceof TextMessage) {
                    try {
                        String text = ((TextMessage) message).getText();
                        if ("shutdown".equals(text)) {
                            latch.countDown();
                        } else {
                            count.addAndGet(NumberUtils.toInt(text.substring(0, text.indexOf(' '))));
                        }
                        //取样输出
                        //if (count.get() % 10 == 0) {
                        logger.info("count:{},msg:{}", count.get(), text);
                        //}
                    } catch (JMSException e) {
                        logger.error("exception.", e);
                    }
                }
            }
        });

        //mock生产者在不停地生产消息
        Producer producer = Producer.createDefault("test.producer.exception", DEV_BETA_URL);
        for (int i = 0; i < 50; i++) {
            String random = RandomStringUtils.random(5, true, true);
            producer.sendMessage(i + " " + random);
            TimeUnit.SECONDS.sleep(2);
        }
        producer.sendMessage("shutdown");

        TimeUnit.SECONDS.sleep(10);

        int n = 0;
        for (int i = 0; i < 50; i++) {
            n += i;
        }

        logger.info("n:{},count:{}", n, count.get());
        latch.await();
        producer.close();
        consumer.close();
    }

    /**
     * 1.异步模式下,broker down掉几分钟,消息是否能全部正确消费！
     * 能,无消息丢失,如果超出缓存,仍会有消息丢失.
     * 2.pool queue中有消息,关闭,能否发完！能,无消息丢失
     */
    @Test
    public void testAsyncMessage() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        MqHelper.getMessage(1, "test.producer.reliable", new MessageProcessor(false) {
            @Override
            public void processMessage(Message message) {
                String text = this.getStringFromTextMessage(message);
                if (StringUtils.equals(text, "shutdown")) {
                    logger.info("enter countdown");
                    latch.countDown();
                } else {
                    count.addAndGet(NumberUtils.toInt(text.substring(0, text.indexOf(' '))));
                }
                //取样输出
                //if (count.get() % 100 == 0) {
                logger.info("count:{},msg:{}", count.get(), text);
                //}
            }
        });

        //mock生产者在不停地生产消息
        for (int i = 0; i < 1000; i++) {
            String random = RandomStringUtils.random(5, true, true);
            MqHelper.asyncSendMessage(1, "test.producer.reliable", i + " " + random);
            //TimeUnit.MILLISECONDS.sleep(50);
        }

        MqHelper.asyncSendMessage(1, "test.producer.reliable", "shutdown");
        //TimeUnit.SECONDS.sleep(10);

        int n = 0;
        for (int i = 0; i < 1000; i++) {
            n += i;
        }

        logger.info("n:{},count:{}", n, count.get());

        try {
            latch.await();
        } catch (Exception e) {
            logger.error("error", e);
        }

        MqHelper.closeProducer();
        MqHelper.closeConsumer();
    }

    /**
     * 测试异步发送,不能发送消息时的Reject策略
     * 首先,异步发送非常快,100条耗时482.00ms,同步发耗费23.98s
     *
     * @throws InterruptedException
     */
    @Test
    public void testAsyncMessageReject() throws InterruptedException {
        //mock生产者在不停地生产消息,中断线路
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < 30000; i++) {
            String random = RandomStringUtils.random(50, true, true);
            MqHelper.asyncSendMessage(1, "test.producer.reject", i + " " + random);
            TimeUnit.MILLISECONDS.sleep(10);
            //MqHelper.sendMessage(1, "test.producer.reject", i + " " + random);
        }
        stopwatch.stop();
        logger.info("send finish cost:{}", stopwatch.toString());

        TimeUnit.SECONDS.sleep(20);
        MqHelper.closeProducer();
    }

}