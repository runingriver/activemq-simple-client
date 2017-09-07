package com.activemq.simple.client.config;

/**
 * 标识类,表示mq日志
 */
public interface MqLog {

}

//logback下将mq日志打印到单独的文件,配置
/*
    <appender name="mqAppender" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <param name="file" value="${catalina.base}/logs/mq.log"/>
        <param name="append" value="true"/>
        <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
            <fileNamePattern>${catalina.base}/logs/mq.%d{yyyy-MM}(%i).log.gz</fileNamePattern>
            <minIndex>1</minIndex>
            <maxIndex>3</maxIndex>
        </rollingPolicy>
        <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
            <MaxFileSize>10MB</MaxFileSize>
        </triggeringPolicy>
        <encoder>
            <Pattern>[%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %logger{36}.%M:%L]-%m%n</Pattern>
            <charset>UTF-8</charset>
        </encoder>
    </appender>
    <logger name="com.activemq.simple.client" additivity="false">
        <level value="INFO"/>
        <appender-ref ref="mqAppender"/>
    </logger>
 */
