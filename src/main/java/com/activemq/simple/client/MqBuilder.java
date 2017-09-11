package com.activemq.simple.client;

import com.activemq.simple.client.config.MqConstant;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.transport.TransportListener;
import org.slf4j.Logger;

/**
 * builder模式创建Producer,Consumer,Publisher,Subscriber
 * 封装创建client对象中共同的部分.
 */
public abstract class MqBuilder {
    protected static final Logger logger = MqConstant.LOG;
    protected ActiveMQConnectionFactory connectionFactory;

    protected String connectionUrl = MqConstant.PRD_SERVER_URL;
    protected String username;
    protected String password;

    protected boolean transacted = false;
    protected int acknowledge = 1;
    protected String destination;
    protected int deliveryMode = 2;

    protected TransportListener transportListener;

    /**
     * 创建一个连接工厂(ActiveMQConnectionFactory)
     *
     * @param username 用户名密码
     * @param password 用户名密码
     * @param url      broker连接字符串
     * @return ConsumerBuilder
     */
    protected MqBuilder createConnectionFactory(String username, String password, String url) {
        logger.info("createConnectionFactory connectionFactory username:{},password:{},url:{}", username, password, url);
        if (url == null || url.isEmpty()) {
            logger.error("connectionUrl is empty when init object!");
            throw new IllegalArgumentException("connection Url string is empty.");
        }
        if (username == null || password == null || username.isEmpty() || password.isEmpty()) {
            connectionFactory = new ActiveMQConnectionFactory(url);
        } else {
            connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        }

        return this;
    }

    /**
     * 创建一个默认的连接工厂(ActiveMQConnectionFactory)
     *
     * @return ConsumerBuilder
     */
    protected MqBuilder createConnectionFactory() {
        this.createConnectionFactory(this.username, this.password, this.connectionUrl);
        return this;
    }

    /**
     * 设置链接字符串,一般不设置,采用默认即可
     *
     * @param url 连接字符串
     * @return MqBuilder
     */
    public MqBuilder setServerUrl(String url) {
        if (url == null || url.isEmpty()) {
            logger.error("connectionUrl is empty when init object!");
            throw new IllegalArgumentException("connection connectionUrl string is empty.");
        }
        this.connectionUrl = url;
        return this;
    }

    /**
     * 设置消息队列的用户名密码
     *
     * @param username 用户密码
     * @param password 用户密码
     * @return MqBuilder
     */
    public MqBuilder setAuthentication(String username, String password) {
        if (username != null && password != null && !username.isEmpty() && !password.isEmpty()) {
            this.username = username;
            this.password = password;
        } else {
            logger.error("username password illegal,username:{},passowrd:{}", username, password);
            throw new IllegalArgumentException("username or password error.");
        }
        return this;
    }

    /**
     * 设置队列名字,如: client.queue.test
     *
     * @param destination 队列名
     * @return MqBuilder
     */
    public MqBuilder setDestinationName(String destination) {
        if (destination == null || destination.isEmpty()) {
            logger.error("destination name is empty when init object!");
            throw new IllegalArgumentException("destination string is empty.");
        }
        this.destination = destination;
        return this;
    }

    /**
     * 是否支持事务,设置true,则acknowledge直接为SESSION_TRANSACTED(不管你设置Acknowledge与否)
     *
     * @param transacted 是否开启是否
     * @return MqBuilder
     */
    public MqBuilder setTransacted(boolean transacted) {
        this.transacted = transacted;
        return this;
    }

    /**
     * 设置消息确认机制
     * 1=AUTO_ACKNOWLEDGE,2=CLIENT_ACKNOWLEDGE,3=DUPS_OK_ACKNOWLEDGE,0=SESSION_TRANSACTED
     *
     * @param acknowledge ack
     * @return MqBuilder
     */
    public MqBuilder setAcknowledge(int acknowledge) {
        if (acknowledge <= 0 || acknowledge > 3) {
            logger.error("acknowledge must int (0,1,2,3),acknowledge={}", acknowledge);
            throw new IllegalArgumentException("acknowledge must int 0,1,2,3.");
        }
        this.acknowledge = acknowledge;
        return this;
    }

    /**
     * 消息发送模式: 2=持久化,1=非持久化
     *
     * @param mode 模式
     * @return MqBuilder
     */
    public MqBuilder setDeliveryMode(int mode) {
        if (mode != 1 && mode != 2) {
            logger.error("delivery mode must int (1,2),mode={}", mode);
            throw new IllegalArgumentException("delivery mode must int 1,2.");
        }
        this.deliveryMode = mode;
        return this;
    }

    /**
     * 为连接添加一个监听器
     * @param listener 监听器
     * @return MqBuilder
     */
    public MqBuilder setTransportListener(TransportListener listener) {
        this.transportListener = listener;
        return this;
    }

    /**
     * 创建一个具体MQ对象
     *
     * @param <T> 具体的Builder类型
     * @return 具体的Builder类型
     */
    public abstract <T> T build();
}
