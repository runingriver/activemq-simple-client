package com.activemq.simple.client;

import com.activemq.simple.client.config.MqConstant;
import org.slf4j.Logger;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

/**
 * 连接建立过程中出现异常监听器
 */
public class MQExceptionListener implements ExceptionListener {
    private static final Logger logger = MqConstant.LOG;

    private String name;

    public MQExceptionListener(String name) {
        this.name = name;
    }

    @Override
    public synchronized void onException(JMSException exception) {
        logger.error("{} connection exception:{}", name, exception.toString());
    }
}
