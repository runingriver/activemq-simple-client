package com.activemq.simple.client;

import com.activemq.simple.client.config.MqConstant;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import java.util.Map;

/**
 * 发布者
 */
public class Publisher {
    private static final Logger logger = MqConstant.LOG;

    private TopicConnection topicConnection;
    private TopicSession topicSession;
    private TopicPublisher publisher;

    public boolean publish(String message) {
        if (message == null || message.isEmpty()) {
            logger.error("parameter of send message is empty.");
            return false;
        }
        try {
            TextMessage textMessage = topicSession.createTextMessage();
            textMessage.setText(message);
            publisher.publish(textMessage);
        } catch (JMSException e) {
            logger.error("send message error.message:{}", message, e);
            return false;
        }
        return true;
    }

    public boolean publish(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            logger.error("parameter of send message map is empty.");
            return false;
        }

        try {
            MapMessage mapMessage = topicSession.createMapMessage();
            Producer.fillMessageMap(mapMessage, map);
            publisher.publish(mapMessage);
        } catch (JMSException e) {
            logger.error("send message error.", e);
            return false;
        }
        return true;
    }

    /**
     * 用完请关闭连接
     */
    public void close() {
        try {
            publisher.close();
        } catch (JMSException e) {
            logger.error("close consumer exception", e);
        }

        try {
            topicSession.close();
        } catch (JMSException e) {
            logger.error("close queueSession exception", e);
        }

        try {
            topicConnection.close();
        } catch (JMSException e) {
            logger.error("close queueConnection exception", e);
        }
    }

    /**
     * 自定义创建一个Publisher.
     *
     * @return PublisherBuilder
     */
    public static PublisherBuilder custom() {
        return new PublisherBuilder();
    }

    /**
     * 创建一个默认的发布者,队列带用户名密码,persistent,auto_acknowledge.
     *
     * @param username  可为null,即不设置用户名密码
     * @param password  可为null,即不设置用户名密码
     * @param url       连接broker的url,null-使用默认
     * @param queueName 队列名,不能为空
     * @return Publisher
     */
    public static Producer createDefault(String username, String password, String url, String queueName) {
        logger.info("create default publisher username:{},password:{},url:{},queue:{}", username, password, url, queueName);
        if (url == null || url.isEmpty()) {
            return new PublisherBuilder().setDestinationName(queueName)
                    .setAuthentication(username, password)
                    .setTransportListener(new MqTransportListener(queueName))
                    .build();
        }
        return new PublisherBuilder().setDestinationName(queueName)
                .setAuthentication(username, password)
                .setServerUrl(url)
                .setTransportListener(new MqTransportListener(queueName))
                .build();
    }

    /**
     * 创建一个默认的发布者,persistent,auto_acknowledge.
     * 不使用用户名密码
     *
     * @param queueName 队列名,不能为空
     * @return Publisher
     */
    public static Publisher createDefault(String queueName) {
        logger.info("create default publisher queue:{}", queueName);
        return new PublisherBuilder()
                .setDestinationName(queueName)
                .setTransportListener(new MqTransportListener(queueName))
                .build();
    }

    /**
     * 创建一个默认的发布者,persistent,auto_acknowledge.
     * 不使用用户名密码,可以指定连接url
     *
     * @param queueName 队列名,不能为空
     * @param url       连接broker的url
     * @return Publisher
     */
    public static Publisher createDefault(String queueName, String url) {
        logger.info("create default publisher url:{},queue:{}", url, queueName);
        PublisherBuilder builder = new PublisherBuilder();
        builder.setDestinationName(queueName)
                .setServerUrl(url)
                .setTransportListener(new MqTransportListener(queueName));
        return builder.build();
    }

    public static final class PublisherBuilder extends MqBuilder {
        private int producerWindowSize = 0;
        private TopicConnection topicConnection;
        private TopicSession topicSession;
        private TopicPublisher publisher;

        private long timeToLive = 0;
        private int priority = 4;

        private String clientId;

        private boolean batchAcknowledge = false;
        private boolean asyncSend = false;
        private boolean copyMessageOnSend = true;
        private boolean compressionMessage = false;

        @Override
        public Publisher build() {
            if (connectionFactory == null) {
                this.createConnectionFactory();
            }
            setConfig();

            try {
                topicConnection = connectionFactory.createTopicConnection();
                topicConnection.start();
                topicSession = topicConnection.createTopicSession(transacted, acknowledge);
                Topic topic = topicSession.createTopic(destination);
                publisher = topicSession.createPublisher(topic);

                publisher.setDeliveryMode(deliveryMode);
                if (timeToLive != 0) {
                    publisher.setTimeToLive(timeToLive);
                }
                if (priority != 4) {
                    publisher.setPriority(priority);
                }
            } catch (JMSException e) {
                logger.error("createConnectionFactory publisher exception.username:{},password:{},url:{},destination:{},transacted:{},acknowledge:{},deliveryMode:{}"
                        , username, password, connectionUrl, destination, transacted, acknowledge, deliveryMode);
                //该异常可能是配置错误,可能是broker错误,转成unchecked exception.
                throw new RuntimeException("createConnectionFactory publisher exception.");
            }

            Publisher publisher = new Publisher();
            publisher.topicConnection = this.topicConnection;
            publisher.topicSession = this.topicSession;
            publisher.publisher = this.publisher;
            return publisher;
        }

        private void setConfig() {
            if (this.producerWindowSize != 0) {
                this.connectionFactory.setProducerWindowSize(this.producerWindowSize);
            }
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
            if (clientId != null) {
                this.connectionFactory.setClientID(clientId);
            }
        }

        /**
         * 给Publisher设置一个ClientId,不要与Subscriber设置为一样的!
         *
         * @param clientId ClientId全局唯一
         * @return PublisherBuilder
         */
        public PublisherBuilder setClientId(String clientId) {
            if (clientId == null || clientId.isEmpty()) {
                throw new IllegalArgumentException("clientId illegal.");
            }
            this.clientId = clientId;
            return this;
        }

        /**
         * 仅针对Topic,请在异步模式下设置, 表示异步发送消息时在broker没有批量返回ack之前,producer能发送给broker的数据大小.
         *
         * @param size 大小
         * @return PublisherBuilder
         */
        public PublisherBuilder setProducerWindowSize(int size) {
            if (size <= 0) {
                throw new IllegalArgumentException("ProducerWindowSize must great 0.");
            }
            this.producerWindowSize = size;
            return this;
        }

        /**
         * 设置批量ack确认,提高系统效率和吞吐量
         * 默认false
         *
         * @param batchAcknowledge 批量返回ack
         * @return PublisherBuilder
         */
        public PublisherBuilder setBatchAcknowledge(boolean batchAcknowledge) {
            this.batchAcknowledge = batchAcknowledge;
            return this;
        }

        /**
         * 设置消息的异步发送.多用于非持久化消息,系统默认异步发送消息,并允许少量消息丢失.
         * 持久化消息,多要求消息的绝对送达,且默认是同步发送.
         * 如果持久化消息,要求提供吞吐量,并能容忍消息的极少量丢失,可以开启异步发送,并设置批量ack
         *
         * @param isAsyncSend 异步发送
         * @return PublisherBuilder
         */
        public PublisherBuilder setAsyncSend(boolean isAsyncSend) {
            this.asyncSend = isAsyncSend;
            return this;
        }

        /**
         * 默认true,在发送消息的时候将一个JMS消息对象拷贝一份,作为消息发送.
         * 如果发送的每个消息都是不同的对象(即不是修改对象的某一部分当做一条新消息发送),可以设置为false
         * false-可以提升性能.
         *
         * @param isCopy 是否拷贝Message对象
         * @return PublisherBuilder
         */
        public PublisherBuilder setCopyMessageOnSend(boolean isCopy) {
            this.copyMessageOnSend = isCopy;
            return this;
        }

        /**
         * 消息发送的时候是否,将消息压缩传送,多用于消息比较大的情况.
         * 默认false
         *
         * @param isCompressionMessage 压缩
         * @return PublisherBuilder
         */
        public PublisherBuilder setCompressionMessage(boolean isCompressionMessage) {
            this.compressionMessage = isCompressionMessage;
            return this;
        }

        /**
         * 设置一条Message发送到broker,存活时长,如果指定时间内没有被消费,则会被丢弃.
         * 默认0,没有限制.
         *
         * @param liveTime 单位:milliseconds,0=没有限制
         * @return PublisherBuilder
         */
        public PublisherBuilder setMessageLiveTime(long liveTime) {
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
         * @return PublisherBuilder
         */
        public PublisherBuilder setProducerMessagePriority(int priority) {
            if (priority < 0 || priority > 9) {
                throw new IllegalArgumentException("priority must int 0~9.");
            }
            this.priority = priority;
            return this;
        }
    }
}
