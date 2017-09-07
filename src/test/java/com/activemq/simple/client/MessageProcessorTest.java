package com.activemq.simple.client;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 主要性能测试.
 * 1. 同一session下多线程处理消息.
 * 2. 多线程建立多session,一个session下一个线程处理消息.
 * 3. 多线程多session, 一个session下多个线程处理消息.
 * 结论: 消息多处理时间短,用类似test1,消息多处理时间长用test3,消息少处理时间长用test1
 * Tip: session消费消息采用从线程池中拿一个线程去执行,另启一个多线程处理session线程的消息,会有很多情况!
 */
public class MessageProcessorTest {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessorTest.class);
    private static final String DEV_BETA_URL = "tcp://127.0.0.1:56161";

    private static final int nThread1 = 10;
    private static final int nThread2 = 20;

    private static final int PENDING_NUMBER = 100;

    /**
     * 结果: 3 ms/per(10 thread);1.51ms/per(20 thread)
     */
    @Test
    public void test1() throws Exception {
        pendingMessageToBroker("test.process.test1", PENDING_NUMBER);

        //确保所有消息已经推送到broker
        TimeUnit.SECONDS.sleep(5);

        Consumer consumer = Consumer.createDefault("test.process.test1", DEV_BETA_URL);
        final CountDownLatch latch = new CountDownLatch(1);
        Stopwatch stopwatch = Stopwatch.createStarted();
        consumer.receiveMessage(new MessageProcessor(nThread2) {
            @Override
            public void processMessage(Message message) {
                if (message instanceof TextMessage) {
                    String text = getStringFromTextMessage(message);
                    doMessage(text);
                    if (StringUtils.equals("shutdown", text)) {
                        latch.countDown();
                    }
                }
            }
        });

        latch.await();
        stopwatch.stop();
        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        logger.info("test1 cost time:{},{} ms/per", stopwatch.toString(), (double) elapsed / PENDING_NUMBER);

        consumer.close();
    }

    /**
     * 结果: 3 ms/per
     * 非严格测试,可能shutdown完成,仍然有线程还在消费buffer中的消息.
     * prefetch,设置影响不大.
     */
    @Test
    public void test2() throws Exception {
        pendingMessageToBroker("test.process.test2", PENDING_NUMBER);
        //确保所有消息已经推送到broker
        TimeUnit.SECONDS.sleep(5);

        //创建nThread个实例
        List<Consumer> consumerList = Lists.newArrayList();
        for (int i = 0; i < 2; i++) {
            Consumer consumer = Consumer.custom().setPrefetchSize(10)
                    .setServerUrl(DEV_BETA_URL)
                    .setDestinationName("test.process.test2").build();
            consumerList.add(consumer);
        }

        final CountDownLatch latch = new CountDownLatch(1);
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (final Consumer consumer : consumerList) {
            try {
                consumer.receiveMessage(new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        if (message instanceof TextMessage) {
                            try {
                                String text = ((TextMessage) message).getText();
                                doMessage(text);
                                if (StringUtils.equals("shutdown", text)) {
                                    latch.countDown();
                                }
                            } catch (JMSException e) {
                                logger.error("transfer message error.");
                            }
                        }
                    }
                });
            } catch (JMSException e) {
                logger.error("receive message error.");
            }
        }

        latch.await();
        stopwatch.stop();
        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        logger.info("test1 cost time:{},{} ms/per", stopwatch.toString(), (double) elapsed / PENDING_NUMBER);

        for (Consumer consumer : consumerList) {
            consumer.close();
        }

    }

    @Test
    public void test3() throws Exception {
        pendingMessageToBroker("test.process.test3", PENDING_NUMBER);
        //确保所有消息已经推送到broker
        TimeUnit.SECONDS.sleep(10);

        //创建nThread个实例
        List<Consumer> consumerList = Lists.newArrayList();
        for (int i = 0; i < nThread1; i++) {
            Consumer consumer = Consumer.custom().setPrefetchSize(10)
                    .setServerUrl(DEV_BETA_URL)
                    .setDestinationName("test.process.test3").build();
            consumerList.add(consumer);
        }

        final CountDownLatch latch = new CountDownLatch(1);
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (final Consumer consumer : consumerList) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        consumer.receiveMessage(new MessageProcessor() {
                            @Override
                            public void processMessage(Message message) {
                                if (message instanceof TextMessage) {
                                    String text = getStringFromTextMessage(message);

                                    if (StringUtils.equals(text, "shutdown")) {
                                        latch.countDown();
                                    } else {
                                        doMessage(text);
                                    }
                                }
                            }
                        });
                    } catch (JMSException e) {
                        logger.error("exception.");
                    }
                }
            });
            thread.start();
        }

        latch.await();
        stopwatch.stop();
        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        logger.info("test3 cost time:{},{} ms/per", stopwatch.toString(), (double) elapsed / PENDING_NUMBER);

        for (Consumer consumer : consumerList) {
            consumer.close();
        }
    }

    public void pendingMessageToBroker(String queueName, int count) {
        Producer throughtput = Producer.createThroughtput(queueName, DEV_BETA_URL);
        logger.info("producer send start.");
        for (int i = 0; i < count; i++) {
            String random = RandomStringUtils.random(10, true, true);
            throughtput.sendMessage(i + " " + random);
        }
        throughtput.sendMessage("shutdown");

        throughtput.close();
        logger.info("producer send end.");
    }

    public void doMessage(String message) {

        try {
            TimeUnit.MILLISECONDS.sleep(3000);
        } catch (InterruptedException e) {
            logger.error("process message error.");
        }

        logger.info("{},{}", message, Thread.currentThread().getName());
    }

    @Test
    public void testExceptionMessage() throws InterruptedException, JMSException {
        //首先生产N条消息
        pendingMessageToBroker("test.process.test4", PENDING_NUMBER);
        //确保所有消息已经推送到broker
        TimeUnit.SECONDS.sleep(1);

        //建立消费者
        Consumer consumer = Consumer.createDefault("test.process.test4", DEV_BETA_URL);
        final CountDownLatch latch = new CountDownLatch(1);
        consumer.receiveMessage(new MessageProcessor(false) {
            @Override
            public void processMessage(Message message) {
                if (message instanceof TextMessage) {
                    String text = getStringFromTextMessage(message);
                    if (StringUtils.equals("shutdown", text)) {
                        latch.countDown();
                    } else {
                        //模拟消费异常
                        throw new RuntimeException("mock unchecked exception.");
                    }
                }
            }
        });
        latch.await();
    }

    private static AtomicInteger count = new AtomicInteger(0);

    /**
     * 模拟消费过程服务restart或down掉,是否有消息丢失
     * 消费过程中,关闭单元测试,模拟服务down掉.
     * 结果: 服务down掉,消息无丢失.
     * @throws InterruptedException
     * @throws JMSException
     */
    @Test
    public void testMessageMissing() throws InterruptedException, JMSException {
        //首先生产N条消息
        //pendingMessageToBroker("test.process.test5", 100000);
        //确保所有消息已经推送到broker
        //TimeUnit.SECONDS.sleep(1);

        //建立消费者
        final Consumer consumer = Consumer.createDefault("test.process.test5", DEV_BETA_URL);
        final CountDownLatch latch = new CountDownLatch(1);
        MessageProcessor processor = new MessageProcessor(1) {
            @Override
            public void processMessage(Message message) {
                if (message instanceof TextMessage) {
                    String text = getStringFromTextMessage(message);

                    if (StringUtils.equals("shutdown", text)) {
                        latch.countDown();
                    } else {
                        String number = text.substring(0, text.indexOf(' '));
                        count.addAndGet(NumberUtils.toInt(number, 0));
                        logger.info("{},{}", count.get(), text);

                        try {
                            TimeUnit.MILLISECONDS.sleep(10);
                        } catch (InterruptedException e) {
                            logger.error("exception.");
                        }
                    }
                }
            }
        };
        consumer.receiveMessage(processor);
        latch.await();
        logger.info("{}", processor.toString());
        int n = 0;
        for (int i = 0; i < 100000; i++) {
            n += i;
        }

        logger.info("count:{},n:{}", count.get(), n);
    }
}