package com.activemq.simple.client;

import com.activemq.simple.client.config.MqConstant;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.slf4j.Logger;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

/**
 * 订阅者
 */
public class Subscriber {
    private static final Logger logger = MqConstant.LOG;

    private TopicConnection topicConnection;
    private TopicSession topicSession;
    private TopicSubscriber subscriber;

    public void receiveMessage(MessageListener listener) throws JMSException {
        try {
            subscriber.setMessageListener(listener);
        } catch (JMSException e) {
            logger.error("register the MessageListener exception.", e);
            throw new JMSException("register the MessageListener exception.");
        }
    }

    /**
     * 用完请关闭连接
     */
    public void close() {
        try {
            subscriber.close();
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
     * 自定义创建一个订阅者
     *
     * @return SubscriberBuilder
     */
    public static SubscriberBuilder custom() {
        logger.info("create consumer by custom.");
        return new SubscriberBuilder();
    }

    /**
     * 创建一个默认的No-durable订阅者,队列带用户名密码,prefetch=1000.
     *
     * @param username  可为null,即队列不需要用户名密码
     * @param password  可为null,即队列不需要用户名密码
     * @param url       连接broker的url,null-使用默认
     * @param queueName 队列名,不能为空
     * @return Subscriber
     */
    public static Subscriber createDefault(String username, String password, String url, String queueName) {
        logger.info("create default subscriber username:{},password:{},url:{},queue:{}", username, password, url, queueName);
        SubscriberBuilder builder = new SubscriberBuilder();
        builder.setAuthentication(username, password).setDestinationName(queueName);
        if (url == null || url.isEmpty()) {
            return builder.build();
        }
        builder.setServerUrl(url);
        return builder.build();
    }

    /**
     * 创建一个默认的No-durable订阅者,prefetch 自定义.
     * 不使用用户名密码
     *
     * @param queueName 队列名,不能为空
     * @return Subscriber
     */
    public static Subscriber createDefault(String queueName) {
        logger.info("create default subscriber topic:{}", queueName);
        SubscriberBuilder builder = new SubscriberBuilder();
        builder.setDestinationName(queueName);
        return builder.build();
    }

    /**
     * 创建一个默认的No-durable订阅者,prefetch 自定义.
     * 不使用用户名密码,可以指定连接broker的url
     *
     * @param queueName 队列名
     * @param url       连接url
     * @return Subscriber
     */
    public static Subscriber createDefault(String queueName, String url) {
        logger.info("create default subscriber topic:{},url:{}", queueName, url);
        SubscriberBuilder builder = new SubscriberBuilder();
        builder.setDestinationName(queueName).setServerUrl(url);
        return builder.build();
    }

    /**
     * 创建一个Durable订阅者,当subscriber重新建立,未送达给当前subscriber的消息会重新发送.
     * 持久化方式默认:persistent
     *
     * @param queueName     队列名,或者叫做主题名
     * @param url           连接broker url
     * @param clientId      clientId和subscriptionName唯一标志一个durable连接
     * @param subscribeName clientId和subscriptionName唯一标志一个durable连接
     * @return Subscriber
     */
    public static Subscriber createDurable(String queueName, String url, String clientId, String subscribeName) {
        logger.info("create durable subscriber topic:{},url:{},clientId:{},subscribeName:{}", queueName, url, clientId, subscribeName);
        SubscriberBuilder builder = new SubscriberBuilder();
        builder.setDestinationName(queueName).setServerUrl(url);
        builder.setClientId(clientId).setSubscribeName(subscribeName);
        return builder.build();
    }

    public static Subscriber createDurable(String username, String password, String url, String queueName, String clientId, String subscribeName) {
        logger.info("create durable subscriber topic:{},url:{},clientId:{},subscribeName:{}", queueName, url, clientId, subscribeName);
        logger.info("---username:{},password:{}", username, password);
        SubscriberBuilder builder = new SubscriberBuilder();
        builder.setAuthentication(username, password).setDestinationName(queueName);
        builder.setClientId(clientId).setSubscribeName(subscribeName);
        if (url == null || url.isEmpty()) {
            return builder.build();
        }
        builder.setServerUrl(url);
        return builder.build();
    }

    public TopicSubscriber getSubscriber() {
        return subscriber;
    }

    public static final class SubscriberBuilder extends MqBuilder {
        private TopicConnection topicConnection;
        private TopicSession topicSession;
        private TopicSubscriber subscriber;

        private String clientId;
        private String subscribeName;

        private int prefetchSize = -1;
        private boolean dispatchAsync = true;

        @Override
        public Subscriber build() {
            if (connectionFactory == null) {
                this.createConnectionFactory();
            }
            setConfig();

            try {
                topicConnection = connectionFactory.createTopicConnection();
                connectionFactory.setExceptionListener(new MqExceptionListener("subscriber connectionFactory"));
                topicConnection.start();

                //设置连接出现异常监听,根据需要看是否需要重建连接.
                topicConnection.setExceptionListener(new MqExceptionListener("subscriber topicConnection"));
                topicSession = topicConnection.createTopicSession(transacted, acknowledge);
                Topic topic = topicSession.createTopic(destination);

                if (subscribeName != null) {
                    subscriber = topicSession.createDurableSubscriber(topic, subscribeName);
                } else {
                    subscriber = topicSession.createSubscriber(topic);
                }
            } catch (JMSException e) {
                logger.error("createConnectionFactory subscriber exception.username:{},password:{},url:{},destination:{},transacted:{},acknowledge:{},deliveryMode:{}"
                        , username, password, connectionUrl, destination, transacted, acknowledge, deliveryMode);
                logger.error("--- clientId:{},subscribeName:{}", clientId, subscribeName);
                //该异常可能是配置错误,可能是broker错误,转成unchecked exception.
                throw new RuntimeException("createConnectionFactory subscriber exception.");
            }

            Subscriber subscriber = new Subscriber();
            subscriber.topicConnection = this.topicConnection;
            subscriber.topicSession = this.topicSession;
            subscriber.subscriber = this.subscriber;
            return subscriber;
        }

        private void setConfig() {
            if (transportListener != null) {
                this.connectionFactory.setTransportListener(transportListener);
            }
            if (clientId != null) {
                //在factory和connection中设置都一样!
                this.connectionFactory.setClientID(clientId);
            }
            if (prefetchSize != -1) {
                ActiveMQPrefetchPolicy policy = new ActiveMQPrefetchPolicy();
                policy.setAll(prefetchSize);
                this.connectionFactory.setPrefetchPolicy(policy);
            }
            if (!dispatchAsync) {
                this.connectionFactory.setDispatchAsync(false);
            }
        }

        /**
         * Durable topic 设置的唯一标识
         *
         * @param clientId 唯一标识
         * @return SubscriberBuilder
         */
        public SubscriberBuilder setClientId(String clientId) {
            if (clientId == null || clientId.isEmpty()) {
                throw new IllegalArgumentException("clientId illegal.");
            }
            this.clientId = clientId;
            return this;
        }

        /**
         * Durable topic 必须设置的唯一标识
         *
         * @param name SubscribeName
         * @return SubscriberBuilder
         */
        public SubscriberBuilder setSubscribeName(String name) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("subscribe name illegal.");
            }
            this.subscribeName = name;
            return this;
        }

        /**
         * 设置消息消费者,本地缓存消息数量.
         * 0=不缓存,定时去broker pull消息,效率低,消息不能及时送达.
         * 1= 缓存一条消息,broker主动push到client,多用于耗时的消息处理.
         * >=2 broker主动push,消息堆积情况下,当buffer中达到1/2的size可用空间时,broker则推送1/2的消息到buffer.
         *
         * @param size size
         * @return SubscriberBuilder
         */
        public SubscriberBuilder setPrefetchSize(int size) {
            if (size < 0) {
                logger.error("PrefetchSize must greater then zero.");
                throw new IllegalArgumentException("the subscriber prefetch size must greater 0.");
            }
            this.prefetchSize = size;
            return this;
        }

        /**
         * 消息是否异步发送到consumer client.
         * 默认true.
         *
         * @param isAsync 是否异步分发
         * @return SubscriberBuilder
         */
        public SubscriberBuilder setDispatchAsync(boolean isAsync) {
            this.dispatchAsync = isAsync;
            return this;
        }

    }
}
