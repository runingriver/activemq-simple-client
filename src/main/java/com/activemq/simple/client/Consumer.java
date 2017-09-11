package com.activemq.simple.client;

import com.activemq.simple.client.config.MqConstant;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.slf4j.Logger;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;

/**
 * 消息消费者
 */
public class Consumer {
    private static final Logger logger = MqConstant.LOG;

    private ActiveMQConnectionFactory connectionFactory;
    private QueueConnection queueConnection;
    private MessageConsumer consumer;
    private QueueSession queueSession;

    public void receiveMessage(MessageListener listener) throws JMSException {
        try {
            consumer.setMessageListener(listener);
            logger.info("set message listener success,queue:{}", listener.toString());
        } catch (JMSException e) {
            logger.error("register the MessageListener exception.", e);
            throw new JMSException("register the MessageListener exception.");
        }
    }

    public MessageConsumer getConsumer() {
        return this.consumer;
    }

    public ActiveMQConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public QueueConnection getQueueConnection() {
        return queueConnection;
    }

    public QueueSession getQueueSession() {
        return queueSession;
    }

    public void close() {
        try {
            consumer.close();
        } catch (JMSException e) {
            logger.error("close consumer exception", e);
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

    public static ConsumerBuilder custom() {
        logger.info("create consumer by custom.");
        return new ConsumerBuilder();
    }

    /**
     * 创建一个默认的消费者,队列带用户名密码,prefetch=1000.
     *
     * @param username  可为null,即队列不需要用户名密码
     * @param password  可为null,即队列不需要用户名密码
     * @param url       连接broker的url,null-使用默认
     * @param queueName 队列名,不能为空
     * @return Consumer
     */
    public static Consumer createDefault(String username, String password, String url, String queueName) {
        logger.info("create default consumer username:{},password:{},url:{},queue:{}", username, password, url, queueName);
        ConsumerBuilder builder = new ConsumerBuilder();
        builder.setAuthentication(username, password)
                .setDestinationName(queueName);
        if (url == null || url.isEmpty()) {
            return builder.build();
        }
        builder.setServerUrl(url);
        return builder.build();
    }

    /**
     * 创建一个默认的消费者,prefetch 自定义.
     * 不使用用户名密码
     *
     * @param queueName 队列名,不能为空
     * @return Consumer
     */
    public static Consumer createDefault(String queueName) {
        logger.info("create default consumer queue:{}", queueName);
        ConsumerBuilder builder = new ConsumerBuilder();
        builder.setDestinationName(queueName);
        return builder.build();
    }

    /**
     * 创建一个默认的消费者,prefetch 自定义.
     * 不使用用户名密码,可以指定连接broker的url
     *
     * @param queueName 队列名
     * @param url       连接url
     * @return Consumer
     */
    public static Consumer createDefault(String queueName, String url) {
        logger.info("create default consumer queue:{},url:{}", queueName, url);
        ConsumerBuilder builder = new ConsumerBuilder();
        builder.setDestinationName(queueName)
                .setServerUrl(url);
        return builder.build();
    }

    /**
     * 创建一个消费消息较慢的消费者,队列带用户名密码,prefetch=1000.
     * 多用于一条消息消费较慢(处理时间较长)的情况.也可用于直接指定prefetch(队列消息较少情况,节约资源)!
     *
     * @param queueName 队列名,不能为空
     * @param size      预取缓冲区大小,处理时间较长且消息较少size=1,消息较多size=10~20
     * @return Consumer
     */
    public static Consumer createHeavyMessage(String queueName, int size) {
        logger.info("create heavy consumer queue:{},size:{}", queueName, size);
        ConsumerBuilder builder = new ConsumerBuilder();
        builder.setDestinationName(queueName);

        return builder.setPrefetchSize(size).build();
    }

    /**
     * 创建一个消费消息较慢的消费者,队列带用户名密码,prefetch=1000.
     * 多用于一条消息消费较慢(处理时间较长)的情况.也可用于直接指定prefetch(队列消息较少情况,节约资源)!
     *
     * @param queueName 队列名,不能为空
     * @param url       预取缓冲区大小,处理时间较长且消息较少size=1,消息较多size=10~20
     * @param size      prefetch大小
     * @return Consumer
     */
    public static Consumer createHeavyMessage(String queueName, String url, int size) {
        logger.info("create heavy consumer queue:{},size:{}", queueName, size);
        ConsumerBuilder builder = new ConsumerBuilder();
        builder.setDestinationName(queueName).setServerUrl(url);

        return builder.setPrefetchSize(size).build();
    }

    /**
     * 创建一个消费消息较慢的消费者,队列带用户名密码,prefetch=1000.
     * 多用于一条消息消费较慢(处理时间较长)的情况.
     * 也可用于队列消息较少情况,节约资源!
     *
     * @param username  可为null,即队列不需要用户名密码
     * @param password  可为null,即队列不需要用户名密码
     * @param url       连接broker的url,null-使用默认
     * @param queueName 队列名,不能为空
     * @param size      预取缓冲区大小,处理时间较长且消息较少size=1,消息较多size=10~20
     * @return Consumer
     */
    public static Consumer createHeavyMessage(String username, String password, String url, String queueName, int size) {
        logger.info("create heavy consumer username:{},password:{},url:{},queue:{},size:{}", username, password, url, queueName, size);
        ConsumerBuilder builder = new ConsumerBuilder();
        builder.setAuthentication(username, password).setDestinationName(queueName);
        if (url == null || url.isEmpty()) {
            return builder.setPrefetchSize(size).build();
        }
        builder.setServerUrl(url);
        return builder.setPrefetchSize(size).build();
    }

    public static class ConsumerBuilder extends MqBuilder {
        private QueueConnection queueConnection;
        private QueueSession queueSession;
        private MessageConsumer consumer;

        private int prefetchSize = -1;
        private boolean dispatchAsync = true;
        private boolean optimizedMessageDispatch = true;
        private boolean nonBlockingRedelivery = false;

        /**
         * 创建一个Consumer对象.接收消息.
         *
         * @return Consumer
         */
        public Consumer build() {
            if (connectionFactory == null) {
                this.createConnectionFactory();
            }
            setConfig();

            try {
                queueConnection = connectionFactory.createQueueConnection();
                queueConnection.start();

                //设置连接出现异常监听,根据需要看是否需要重建连接.
                queueConnection.setExceptionListener(new MqExceptionListener("consumer queueConnection"));
                queueSession = queueConnection.createQueueSession(transacted, acknowledge);
                Queue queue = queueSession.createQueue(destination);
                consumer = queueSession.createConsumer(queue);
            } catch (JMSException e) {
                logger.error("createConnectionFactory consumer exception.username:{},password:{},url:{},destination:{},transacted:{},acknowledge:{},deliveryMode:{}"
                        , username, password, connectionUrl, destination, transacted, acknowledge, deliveryMode, e);
                //该异常可能是配置错误,可能是broker错误,转成unchecked exception.
                throw new RuntimeException("createConnectionFactory producer exception.");
            }

            Consumer consumer = new Consumer();
            consumer.connectionFactory = this.connectionFactory;
            consumer.consumer = this.consumer;
            consumer.queueConnection = this.queueConnection;
            consumer.queueSession = this.queueSession;
            return consumer;
        }

        private void setConfig() {
            if (prefetchSize != -1) {
                ActiveMQPrefetchPolicy policy = new ActiveMQPrefetchPolicy();
                policy.setAll(prefetchSize);
                this.connectionFactory.setPrefetchPolicy(policy);
            }
            if (!dispatchAsync) {
                this.connectionFactory.setDispatchAsync(false);
            }
            if (!optimizedMessageDispatch) {
                this.connectionFactory.setOptimizedMessageDispatch(false);
            }
            if (nonBlockingRedelivery) {
                this.connectionFactory.setNonBlockingRedelivery(true);
            }
            if (transportListener != null) {
                this.connectionFactory.setTransportListener(transportListener);
            }
        }

        /**
         * 设置消息消费者,本地缓存消息数量.
         * 0=不缓存,定时去broker pull消息,效率低,消息不能及时送达.
         * 1= 缓存一条消息,broker主动push到client,多用于耗时的消息处理.
         * >=2 broker主动push,消息堆积情况下,当buffer中达到1/2的size可用空间时,broker则推送1/2的消息到buffer.
         *
         * @param size size
         * @return ConsumerBuilder
         */
        public ConsumerBuilder setPrefetchSize(int size) {
            if (size < 0) {
                logger.error("PrefetchSize must greater then zero.");
                throw new IllegalArgumentException("the consume prefetch size must greater 0.");
            }
            this.prefetchSize = size;
            return this;
        }

        /**
         * 消息是否异步发送到consumer client.
         * 默认true.
         *
         * @param isAsync 是否异步分发
         * @return ConsumerBuilder
         */
        public ConsumerBuilder setDispatchAsync(boolean isAsync) {
            this.dispatchAsync = isAsync;
            return this;
        }

        /**
         * 仅在持久化Topic消息有效.默认=true
         * 通过提升Prefetch大小提高性能,注意与setPrefetchSize的设置冲突.
         *
         * @return ConsumerBuilder
         */
        public ConsumerBuilder setOptimizedMessageDispatch(boolean optimized) {
            this.optimizedMessageDispatch = optimized;
            return this;
        }

        /**
         * 设置当事务回滚时,是否阻塞其他消息发送到该消费者,提升性能,
         * 默认false,即阻塞.一般请保持默认.
         *
         * @param isNonBlock 阻塞
         * @return ConsumerBuilder
         */
        public ConsumerBuilder setNonBlockingRedelivery(boolean isNonBlock) {
            this.nonBlockingRedelivery = isNonBlock;
            return this;
        }
    }
}
