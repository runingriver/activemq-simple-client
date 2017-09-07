package com.activemq.simple.client;

import com.activemq.simple.client.config.MqConstant;
import org.apache.activemq.transport.TransportListener;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * 连接的监听器,可以根据业务情况,监听到异常后进行相应处理
 * Consumer不要设置该Listener,每条消息会有一个onCommand回调!
 */
public class MQTransportListener implements TransportListener {
    private static final Logger logger = MqConstant.LOG;
    private String listenerName;
    public static volatile boolean isConnected = true;

    public MQTransportListener(String listenerName) {
        this.listenerName = listenerName;
    }

    /**
     * 连接建立的时候触发,每建立一条连接对应三次onCommand回调
     *
     * @param command 回调传入,object中包含非常详细的各种信息
     */
    @Override
    public void onCommand(Object command) {
        logger.info("{} Transport Listener onCommand obj:{}", listenerName, command);
        isConnected = true;
        logger.info("set connected state to be build,isConnected:{}", isConnected);
    }

    @Override
    public void onException(IOException error) {
        logger.error("{} Transport Listener exception occur:{}", listenerName, error);
    }

    /**
     * 连接丢失时触发,发送在任何消息处理之前
     */
    @Override
    public void transportInterupted() {
        logger.info("{} Transport Listener transport Interrupted.", listenerName);
        isConnected = false;
        logger.info("change connected state to lost connected,isConnected:{}", isConnected);
    }

    /**
     * 连接重新建立触发
     */
    @Override
    public void transportResumed() {
        logger.info("{} Transport Listener transport Resumed.", listenerName);
        isConnected = true;
        logger.info("change connected state to reconnected,isConnected:{}", isConnected);
    }
}

