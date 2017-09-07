package com.activemq.simple.client.config;

import com.activemq.simple.client.Producer;
import com.activemq.simple.client.Consumer;
import com.activemq.simple.client.MessageProcessor;
import org.slf4j.Logger;

import javax.jms.JMSException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进一步封装消息发送方式
 */
public class MqHelper {
    private static final Logger logger = MqConstant.LOG;

    public static final int TEST = 1;
    public static final int PROD = 2;

    private static volatile Map<String, Producer> producerMap = new ConcurrentHashMap<String, Producer>();

    private static volatile Map<String, Consumer> consumerMap = new ConcurrentHashMap<String, Consumer>();

    /**
     * 往指定队列中发送一条消息
     * 指定环境:env=MqHelper.TEST/env=MqHelper.PROD
     *
     * @param env       env=MqHelper.TEST/env=MqHelper.PROD
     * @param queueName 消息发往的队列
     * @param content   消息内容,一般json串
     * @return true-成功,false-失败
     */
    public static boolean sendMessage(int env, String queueName, String content) {
        if (env != 2 && env != 1) {
            throw new IllegalArgumentException("parameter env illegal.");
        }

        if (queueName == null || queueName.isEmpty() || content == null || content.isEmpty()) {
            throw new IllegalArgumentException("parameter queueName or content illegal.");
        }

        Producer producer;
        if (producerMap.containsKey(queueName)) {
            producer = producerMap.get(queueName);
            return producer.sendMessage(content);
        }

        logger.info("create a queue connection for queue:{},env:{}(1=test,2=prod)", queueName, env);
        synchronized (MqHelper.class) {
            if (producerMap.containsKey(queueName)) {
                producer = producerMap.get(queueName);
            } else {
                if (env == 1) {
                    producer = Producer.createDefault(queueName, MqConstant.DEV_BETA_SERVER_URL);
                } else {
                    producer = Producer.createDefault(MqConstant.PRD_USERNAME, MqConstant.PRD_PASSWORD, MqConstant.PRD_SERVER_URL, queueName);
                }
                producerMap.put(queueName, producer);
            }
        }
        return producer.sendMessage(content);
    }

    /**
     * 提供独立线程池异步发送消息,且当broker,connection短暂不可用时保证消息不丢失
     *
     * @param env       env=MqHelper.TEST/env=MqHelper.PROD
     * @param queueName 消息发往的队列
     * @param content   消息内容,一般json串
     */
    public static void asyncSendMessage(int env, String queueName, String content) {
        if (env != 2 && env != 1) {
            throw new IllegalArgumentException("parameter env illegal.");
        }

        if (queueName == null || queueName.isEmpty() || content == null || content.isEmpty()) {
            throw new IllegalArgumentException("parameter queueName or content illegal.");
        }

        Producer producer;
        if (producerMap.containsKey(queueName)) {
            producer = producerMap.get(queueName);
            producer.sendImportantMessage(content);
            return;
        }

        logger.info("create important msg queue connection for queue:{},env:{}(1=test,2=prod)", queueName, env);
        synchronized (MqHelper.class) {
            if (producerMap.containsKey(queueName)) {
                producer = producerMap.get(queueName);
            } else {
                if (env == 2) {
                    producer = Producer.createDefault(MqConstant.PRD_USERNAME, MqConstant.PRD_PASSWORD, MqConstant.PRD_SERVER_URL, queueName);
                } else {
                    producer = Producer.createDefault(queueName, MqConstant.DEV_BETA_SERVER_URL);
                }
                producer.initSendMessagePool();
                producerMap.put(queueName, producer);
            }
        }
        producer.sendImportantMessage(content);
    }

    /**
     * 接收string类型的消息,processor如果使用线程池需要自己close,保证消息无丢失
     * 指定环境:env=MqHelper.TEST/env=MqHelper.PROD
     *
     * @param env       env=MqHelper.TEST/env=MqHelper.PROD
     * @param queueName 消费的队列
     * @param processor 处理逻辑,new MessageProcessor的时候可以指定开启的线程数,默认10个线程处理消息
     */
    public static void getMessage(int env, String queueName, MessageProcessor processor) {
        if (env != 1 && env != 2) {
            throw new IllegalArgumentException("parameter env illegal.");
        }

        if (queueName == null || queueName.isEmpty() || processor == null) {
            throw new IllegalArgumentException("parameter queueName or processor illegal.");
        }

        Consumer consumer;
        if (consumerMap.containsKey(queueName)) {
            consumer = consumerMap.get(queueName);
        } else {
            logger.info("create a queue connection for queue:{},env:{}(1=test,2=prod)", queueName, env);
            synchronized (MqHelper.class) {
                if (consumerMap.containsKey(queueName)) {
                    consumer = consumerMap.get(queueName);
                } else {
                    if (env == 1) {
                        consumer = Consumer.createDefault(queueName, MqConstant.DEV_BETA_SERVER_URL);
                    } else {
                        consumer = Consumer.createDefault(MqConstant.PRD_USERNAME, MqConstant.PRD_PASSWORD, MqConstant.PRD_SERVER_URL, queueName);
                    }
                    consumerMap.put(queueName, consumer);
                }
            }
        }

        try {
            consumer.receiveMessage(processor);
        } catch (JMSException e) {
            logger.error("set receiveMessage exception.", e);
        }
    }

    /**
     * 接收string类型的消息
     *
     * @param queueName 消费的队列
     * @param consumer  自定义消费者
     * @param processor 处理逻辑,new MessageProcessor的时候可以指定开启的线程数,默认10个线程处理消息
     */
    public static void getMessage(String queueName, Consumer consumer, MessageProcessor processor) {
        if (consumer == null) {
            throw new IllegalArgumentException("parameter consumer illegal.");
        }
        if (queueName == null || queueName.isEmpty() || processor == null) {
            throw new IllegalArgumentException("parameter queueName or processor illegal.");
        }
        if (consumerMap.containsKey(queueName)) {
            throw new IllegalArgumentException("the queue is already contains a consumer.");
        }

        consumerMap.put(queueName, consumer);
        try {
            consumer.receiveMessage(processor);
        } catch (JMSException e) {
            logger.error("set receiveMessage exception.", e);
        }
    }

    /**
     * 用于接收比较少量的消息或者消息消费时间较长的队列
     *
     * @param env       env=MqHelper.TEST/env=MqHelper.PROD
     * @param queueName 消费的队列
     * @param size      prefetch的size
     * @param processor 处理逻辑,new MessageProcessor的时候可以指定开启的线程数,默认10个线程处理消息
     */
    public static void getSimpleMessage(int env, String queueName, int size, MessageProcessor processor) {
        if (env != 1 && env != 2) {
            throw new IllegalArgumentException("parameter env illegal.");
        }

        if (queueName == null || queueName.isEmpty() || processor == null) {
            throw new IllegalArgumentException("parameter queueName or processor illegal.");
        }

        Consumer consumer;
        if (consumerMap.containsKey(queueName)) {
            consumer = consumerMap.get(queueName);
        } else {
            logger.info("create a queue connection for queue:{},env:{}(1=test,2=prod)", queueName, env);
            synchronized (MqHelper.class) {
                if (consumerMap.containsKey(queueName)) {
                    consumer = consumerMap.get(queueName);
                } else {
                    if (env == 1) {
                        consumer = Consumer.createHeavyMessage(queueName, MqConstant.DEV_BETA_SERVER_URL, size);
                    } else {
                        consumer = Consumer.createHeavyMessage(MqConstant.PRD_USERNAME, MqConstant.PRD_PASSWORD, MqConstant.PRD_SERVER_URL, queueName, size);
                    }
                    consumerMap.put(queueName, consumer);
                }
            }
        }

        try {
            consumer.receiveMessage(processor);
        } catch (JMSException e) {
            logger.error("set receiveMessage exception.", e);
        }
    }

    /**
     * 不再使用,请执行关闭
     */
    public static void closeProducer() {
        logger.info("close all producer:{}", producerMap.keySet().toString());
        if (!producerMap.isEmpty()) {
            for (Producer producer : producerMap.values()) {
                producer.close();
            }
            producerMap.clear();
        }
    }

    /**
     * 当队列出现未知情况,可以将该生产者关闭,然后重新发送消息(会自动重新创建队列)
     */
    public static void closeProducer(String queueName) {
        logger.info("close producer:{}", queueName);
        if (producerMap.containsKey(queueName)) {
            producerMap.get(queueName).close();
            producerMap.remove(queueName);
        }
    }

    /**
     * 不再使用,请执行关闭
     * 不关闭Processor,当queue重启时,Processor对象需要close,再创建！
     */
    public static void closeConsumer() {
        logger.info("close all consumer:{}", consumerMap.keySet().toString());
        if (!consumerMap.isEmpty()) {
            for (Consumer consumer : consumerMap.values()) {
                consumer.close();
            }
            consumerMap.clear();
        }
    }

    /**
     * 当队列出现未知情况,可以将该消费者关闭,然后重新getMessage,来注册消费者逻辑
     */
    public static void closeConsumer(String queueName) {
        logger.info("close consumer:{}", queueName);
        if (consumerMap.containsKey(queueName)) {
            consumerMap.get(queueName).close();
            consumerMap.remove(queueName);
        }
    }
}
