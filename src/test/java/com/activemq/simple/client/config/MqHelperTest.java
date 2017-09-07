package com.activemq.simple.client.config;

import com.activemq.simple.client.MessageProcessor;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by zongzhehu on 17-6-26.
 */
public class MqHelperTest {
    private static final Logger logger = LoggerFactory.getLogger(MqHelperTest.class);

    @Test
    public void sendMessage() throws Exception {
        for (int i = 0; i < 100; i++) {
            String content = i + " " + RandomStringUtils.random(10, true, true);
            MqHelper.sendMessage(MqHelper.TEST, "test.client.helper", content);
        }
    }

    @Test
    public void getMessage() throws Exception {
        MqHelper.getMessage(MqHelper.TEST, "test.client.helper", new MessageProcessor(1) {
            @Override
            public void processMessage(Message message) {
                if (message instanceof TextMessage) {
                    try {
                        logger.info("receive:{}", ((TextMessage) message).getText());
                    } catch (JMSException e) {
                        logger.error("error.");
                    }
                }
            }
        });

        TimeUnit.SECONDS.sleep(10);

    }


    @Test
    public void closeTest() throws Exception {
        Map<String, String> map = new ConcurrentHashMap<String, String>();
        if (map.isEmpty()) {
            logger.info("init is empty.");
        } else {
            logger.info("init is not empty.");
        }
    }
}