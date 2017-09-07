package com.activemq.simple.client.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 常量
 */
public class MqConstant {
    //线上url连接字符串
    public static final String PRD_SERVER_URL = "failover:(tcp://127.0.0.1:56161)?randomize=false";
    //测试环境url连接字符串
    public static final String DEV_BETA_SERVER_URL = "failover:(tcp://127.0.0.1:56161)";
    public static final String PRD_USERNAME = "admin";
    public static final String PRD_PASSWORD = "admin";
    //统一日志输入,方便将日志输出到独立文件
    public static final Logger LOG = LoggerFactory.getLogger(MqLog.class);
    //缓存5w条消息,预估值:极限1k每条,则占50M内存,容忍500qps下2分钟的故障!
    public static final int CACHE_MESSAGE_SIZE = 100000;

}
