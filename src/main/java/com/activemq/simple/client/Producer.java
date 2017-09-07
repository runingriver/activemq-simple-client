package com.activemq.simple.client;

import com.activemq.simple.client.config.MqConstant;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消息生产者
 */
public class Producer {
    private static final Logger logger = MqConstant.LOG;

    private ActiveMQConnectionFactory connectionFactory;
    private QueueConnection queueConnection;
    private QueueSession queueSession;
    private MessageProducer producer;

    /**
     * 发送String类型的消息
     * 推荐此类消息发送方式,对象请用json.
     *
     * @param message param
     * @return true-发送成功,false=发送异常
     */
    public boolean sendMessage(String message) {
        //一条空消息没有任何意义
        if (message == null || message.isEmpty()) {
            logger.error("parameter of send message is empty.");
            return false;
        }

        try {
            TextMessage textMessage = queueSession.createTextMessage(message);
            producer.send(textMessage);
        } catch (JMSException e) {
            logger.error("send message error.message:{}", message, e);
            return false;
        }
        return true;
    }

    /**
     * 发送重要消息,如果broker,connection等不可用,缓存一部分消息
     * 如果缓冲区满,则丢弃旧的消息
     * 如果大量消息会导致Reject,sleep并重发3次
     *
     * @param message message
     */
    public void sendImportantMessage(final String message) {
        if (message == null || message.isEmpty()) {
            logger.error("parameter of send message is empty.");
            return;
        }
        boolean isSuccess = sendMessageByAsync(message);
        if (isSuccess) {
            return;
        }

        boolean isConnected = MQTransportListener.isConnected;
        if (isConnected) {
            //连接良好,sleep,重发,此不会拖慢整个系统
            int times = 0;
            while (times < 2 && !isSuccess) {
                times++;
                sleep(times * times * 10);
                isSuccess = sendMessageByAsync(message);
            }
            logger.info("pool is full,resend times:{}", times);
        } else {
            //连接中断,记录日志,丢弃,报警
            int count = discardCount.incrementAndGet();
            logger.info("pool is full,discard items:{}, message:{}", count, message);
            if (count % 200 == 1) {
                //do something like alarm
            }
        }
    }

    private boolean sendMessageByAsync(final String message) {
        try {
            sendMessagePool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        TextMessage textMessage = queueSession.createTextMessage(message);
                        producer.send(textMessage);
                    } catch (Exception e) {
                        //if transport exception,failover pattern will blocking.
                        logger.error("send message error.message:{}", message, e);
                    }
                }
            });
        } catch (Exception e) {
            logger.info("pool is full,reject exception occur.");
            return false;
        }
        return true;
    }

    private static final AtomicInteger discardCount = new AtomicInteger(0);
    private static final AtomicInteger poolNumber = new AtomicInteger(0);
    private ThreadPoolExecutor sendMessagePool;

    public void initSendMessagePool() {
        //注意并发问题
        if (sendMessagePool != null) {
            return;
        }
        sendMessagePool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(MqConstant.CACHE_MESSAGE_SIZE),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "MQSendMessage-Thread-" + poolNumber.incrementAndGet());
                    }
                }, new ThreadPoolExecutor.AbortPolicy());

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                closeSendMessagePool();
            }
        }, "Producer-shutdown-hook-thread"));
    }

    private synchronized void closeSendMessagePool() {
        try {
            if (sendMessagePool == null || sendMessagePool.isShutdown() || sendMessagePool.isTerminated()) {
                return;
            }

            logger.info("Producer shutdown,queue size:{}", sendMessagePool.getQueue().size());
            sendMessagePool.shutdown();
            sendMessagePool.awaitTermination(1, TimeUnit.HOURS);
            logger.info("Producer close pool finish,isTerminated:{}", sendMessagePool.isTerminated());
        } catch (Exception e) {
            logger.error("close producer pool exception.", e);
        }
    }


    /**
     * 发送字节数据.
     *
     * @param message param
     * @return true-发送成功,false=发送异常
     */
    public boolean sendMessage(byte[] message) {
        if (message == null) {
            logger.error("parameter of send message is empty.");
            return false;
        }

        try {
            BytesMessage bytesMessage = queueSession.createBytesMessage();
            bytesMessage.writeObject(message);
            producer.send(bytesMessage);
        } catch (JMSException e) {
            logger.error("send byte message error.", e);
            return false;
        }
        return true;
    }

    /**
     * 发送Map类型的消息
     *
     * @param objectMap param
     * @return true-发送成功,false=发送异常
     */
    public boolean sendMessage(Map<String, Object> objectMap) {
        if (objectMap == null || objectMap.isEmpty()) {
            logger.error("parameter of send message map is empty.");
            return false;
        }

        try {
            MapMessage mapMessage = queueSession.createMapMessage();
            fillMessageMap(mapMessage, objectMap);
            producer.send(mapMessage);
        } catch (JMSException e) {
            logger.error("send message error.", e);
            return false;
        }
        return true;
    }

    /**
     * 将普通Map,转换成map类型的消息对象
     */
    public static void fillMessageMap(MapMessage mapMessage, Map<String, Object> objectMap) throws JMSException {
        try {
            for (Map.Entry<String, Object> entry : objectMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof String) {
                    mapMessage.setString(key, String.valueOf(value));
                } else {
                    mapMessage.setObject(key, value);
                }
            }
        } catch (JMSException e) {
            logger.error("createConnectionFactory message map exception.objectMap:{}", objectMap.toString(), e);
            throw new JMSException("createConnectionFactory message map exception.");
        }
    }

    /**
     * 如果设置事务,则提交事务
     *
     * @throws JMSException 事务提交失败
     */
    public void commit() throws JMSException {
        this.queueSession.commit();
    }

    /**
     * 如果事务执行失败,提交回滚事务.
     *
     * @throws JMSException 事务回滚失败
     */
    public void rollback() throws JMSException {
        this.queueSession.rollback();
    }

    public void close() {
        if (sendMessagePool != null && !sendMessagePool.isShutdown()) {
            closeSendMessagePool();
        }

        try {
            producer.close();
        } catch (JMSException e) {
            logger.error("close producer exception", e);
        }

        try {
            queueSession.close();
        } catch (JMSException e) {
            logger.error("close queueSession exception", e);
        }

        try {
            queueConnection.close();
        } catch (JMSException e) {
            logger.error("close queueConnection exception", e);
        }
    }

    private void sleep(int milliseconds) {
        try {
            TimeUnit.MILLISECONDS.sleep(milliseconds);
        } catch (InterruptedException e) {
            logger.error("{} Thread sleep exception", Thread.currentThread().getName(), e);
        }
    }

    public static ProducerBuilder custom() {
        return new ProducerBuilder();
    }

    /**
     * 创建一个默认的生产者,队列带用户名密码,persistent,auto_acknowledge.
     *
     * @param username  可为null,即不设置用户名密码
     * @param password  可为null,即不设置用户名密码
     * @param url       连接broker的url,null-使用默认
     * @param queueName 队列名,不能为空
     * @return Producer
     */
    public static Producer createDefault(String username, String password, String url, String queueName) {
        logger.info("create default producer username:{},password:{},url:{},queue:{}", username, password, url, queueName);
        if (url == null || url.isEmpty()) {
            return new ProducerBuilder().setDestinationName(queueName)
                    .setAuthentication(username, password)
                    .setTransportListener(new MQTransportListener("producer " + queueName))
                    .build();
        }
        return new ProducerBuilder().setDestinationName(queueName)
                .setAuthentication(username, password)
                .setServerUrl(url)
                .setTransportListener(new MQTransportListener("producer " + queueName))
                .build();
    }

    /**
     * 创建一个默认的生产者,persistent,auto_acknowledge.
     * 不使用用户名密码
     *
     * @param queueName 队列名,不能为空
     * @return Producer
     */
    public static Producer createDefault(String queueName) {
        logger.info("create default producer queue:{}", queueName);
        return new ProducerBuilder().setDestinationName(queueName)
                .setTransportListener(new MQTransportListener("producer " + queueName))
                .build();
    }

    /**
     * 创建一个默认的生产者,persistent,auto_acknowledge.
     * 不使用用户名密码,可以指定连接url
     *
     * @param queueName 队列名,不能为空
     * @param url       连接broker的url
     * @return Producer
     */
    public static Producer createDefault(String queueName, String url) {
        logger.info("create default producer url:{},queue:{}", url, queueName);
        ProducerBuilder builder = new ProducerBuilder();
        builder.setDestinationName(queueName)
                .setServerUrl(url)
                .setTransportListener(new MQTransportListener("producer " + queueName));
        return builder.build();
    }

    /**
     * 创建一个高吞吐量的生产者,即:异步批量ack,异步发送,不复制Message对象.
     * 如果进一步提升吞吐量可以设置消息非持久化.
     * 多用于重要性较低的场景.
     *
     * @param username  用户名密码为null-不设置用户名密码
     * @param password  用户名密码为null-不设置用户名密码
     * @param url       指定broker的连接url,null-使用默认.
     * @param queueName 队列名
     * @return Producer
     */
    public static Producer createThroughtput(String username, String password, String url, String queueName) {
        logger.info("create throughtput producer username:{},password:{},url:{},queue:{}", username, password, url, queueName);
        ProducerBuilder builder = new ProducerBuilder();
        builder.setDestinationName(queueName)
                .setAuthentication(username, password)
                .setTransportListener(new MQTransportListener("producer " + queueName));
        if (url != null && !url.isEmpty()) {
            builder.setServerUrl(url);
        }

        //批量ack确认
        builder.setBatchAcknowledge(true);
        //异步发送
        builder.setAsyncSend(true);
        //不持久化
        builder.setDeliveryMode(1);
        //消息不复制,再传.
        builder.setCopyMessageOnSend(false);
        return builder.build();
    }

    /**
     * 创建一个高吞吐量的生产者,即:异步批量ack,异步发送,不复制Message对象.
     * 如果进一步提升吞吐量可以设置消息非持久化.
     * 多用于重要性较低的场景.
     *
     * @param queueName 队列名
     * @return Producer
     */
    public static Producer createThroughtput(String queueName) {
        logger.info("create throughtput producer queue:{}", queueName);
        ProducerBuilder builder = new ProducerBuilder();
        builder.setDestinationName(queueName)
                .setDeliveryMode(1)
                .setTransportListener(new MQTransportListener("producer " + queueName));
        return builder.setBatchAcknowledge(true)
                .setAsyncSend(true)
                .setCopyMessageOnSend(false)
                .build();
    }

    /**
     * 创建一个高吞吐量的生产者,即:异步批量ack,异步发送,不复制Message对象.
     * 如果进一步提升吞吐量可以设置消息非持久化.
     * 多用于重要性较低的场景.
     *
     * @param queueName 队列名
     * @param url       连接broker的url
     * @return Producer
     */
    public static Producer createThroughtput(String queueName, String url) {
        logger.info("create throughtput producer url:{},queue:{}", url, queueName);
        ProducerBuilder builder = new ProducerBuilder();
        builder.setDestinationName(queueName)
                .setServerUrl(url)
                .setTransportListener(new MQTransportListener("producer " + queueName));
        builder.setBatchAcknowledge(true).setAsyncSend(true).setCopyMessageOnSend(false);
        return builder.build();
    }

    public ActiveMQConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public MessageProducer getProducer() {
        return producer;
    }

    public QueueConnection getQueueConnection() {
        return queueConnection;
    }

    public QueueSession getQueueSession() {
        return queueSession;
    }

    public static final class ProducerBuilder extends MQBuilder {
        private QueueConnection queueConnection;
        private QueueSession queueSession;
        private MessageProducer producer;

        private long timeToLive = 0;
        private int priority = 4;

        private boolean batchAcknowledge = false;
        private boolean asyncSend = false;
        private boolean copyMessageOnSend = true;
        private boolean compressionMessage = false;


        /**
         * 创建一个Producer对象.发送消息.
         *
         * @return Producer
         */
        public Producer build() {
            if (connectionFactory == null) {
                this.createConnectionFactory();
            }
            setConfig();

            try {
                queueConnection = connectionFactory.createQueueConnection();
                queueConnection.start();
                queueConnection.setExceptionListener(new MQExceptionListener("producer queueConnection"));
                queueSession = queueConnection.createQueueSession(transacted, acknowledge);
                Queue queue = queueSession.createQueue(destination);
                producer = queueSession.createProducer(queue);
                producer.setDeliveryMode(deliveryMode);
                if (timeToLive != 0) {
                    producer.setTimeToLive(timeToLive);
                }
                if (priority != 4) {
                    producer.setPriority(priority);
                }
            } catch (JMSException e) {
                logger.error("createConnectionFactory producer exception.username:{},password:{},url:{},destination:{},transacted:{},acknowledge:{},deliveryMode:{}"
                        , username, password, connectionUrl, destination, transacted, acknowledge, deliveryMode, e);
                //该异常可能是配置错误,可能是broker错误,转成unchecked exception.
                throw new RuntimeException("createConnectionFactory producer exception.");
            }

            Producer producer = new Producer();
            producer.connectionFactory = this.connectionFactory;
            producer.producer = this.producer;
            producer.queueConnection = this.queueConnection;
            producer.queueSession = this.queueSession;
            return producer;
        }

        private void setConfig() {
            if (batchAcknowledge) {
                this.connectionFactory.setOptimizeAcknowledge(true);
            }
            if (asyncSend) {
                this.connectionFactory.setUseAsyncSend(true);
            }
            if (!copyMessageOnSend) {
                this.connectionFactory.setCopyMessageOnSend(false);
            }
            if (compressionMessage) {
                this.connectionFactory.setUseCompression(true);
            }
            if (transportListener != null) {
                this.connectionFactory.setTransportListener(transportListener);
            }
        }

        /**
         * 设置批量ack确认,提高系统效率和吞吐量
         * 默认false
         *
         * @param batchAcknowledge 批量返回ack
         * @return ProducerBuilder
         */
        public ProducerBuilder setBatchAcknowledge(boolean batchAcknowledge) {
            this.batchAcknowledge = batchAcknowledge;
            return this;
        }

        /**
         * 设置消息的异步发送.多用于非持久化消息,系统默认异步发送消息,并允许少量消息丢失.
         * 持久化消息,多要求消息的绝对送达,且默认是同步发送.
         * 如果持久化消息,要求提供吞吐量,并能容忍消息的极少量丢失,可以开启异步发送,并设置批量ack
         *
         * @param isAsyncSend 异步发送
         * @return ProducerBuilder
         */
        public ProducerBuilder setAsyncSend(boolean isAsyncSend) {
            this.asyncSend = isAsyncSend;
            return this;
        }

        /**
         * 默认true,在发送消息的时候将一个JMS消息对象拷贝一份,作为消息发送.
         * 如果发送的每个消息都是不同的对象(即不是修改对象的某一部分当做一条新消息发送),可以设置为false
         * false-可以提升性能.
         *
         * @param isCopy 是否拷贝Message对象
         * @return ProducerBuilder
         */
        public ProducerBuilder setCopyMessageOnSend(boolean isCopy) {
            this.copyMessageOnSend = isCopy;
            return this;
        }

        /**
         * 消息发送的时候是否,将消息压缩传送,多用于消息比较大的情况.
         * 默认false
         *
         * @param isCompressionMessage 压缩
         * @return ProducerBuilder
         */
        public ProducerBuilder setCompressionMessage(boolean isCompressionMessage) {
            this.compressionMessage = isCompressionMessage;
            return this;
        }

        /**
         * 设置一条Message发送到broker,存活时长,如果指定时间内没有被消费,则会被丢弃.
         * 默认0,没有限制.
         *
         * @param liveTime 单位:milliseconds,0=没有限制
         * @return ProducerBuilder
         */
        public ProducerBuilder setMessageLiveTime(long liveTime) {
            if (liveTime <= 0) {
                return this;
            }
            this.timeToLive = liveTime;
            return this;
        }

        /**
         * 设置生产者发送消息的优先级.
         * 范围[0,9],0-4表示普通优先级,5-9表示高优先级优先加过推送.
         * 默认优先级位4.
         *
         * @param priority 0-9
         * @return ProducerBuilder
         */
        public ProducerBuilder setProducerMessagePriority(int priority) {
            if (priority < 0 || priority > 9) {
                throw new IllegalArgumentException("priority must int 0~9.");
            }
            this.priority = priority;
            return this;
        }
    }

}
