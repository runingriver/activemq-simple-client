# activemq-simple-client jar包说明
1. 对activemq-client jar进行二次封装,并简化activemq的使用!
2. 支持个性化定制consumer,producer的使用场景!
3. 实现consumer多线程消费,解决大量消息单机单消费者消费较慢问题,并保证消息的可靠性!
4. 实现producer在持久化模式下的异步发送,支持broker宕机而消息不丢失,相比使用windowSize的异步发送,具有更高可靠性!
Tip:支持上面两点依据,可以从test中获得,也可以模拟环境测试!

# 使用
1. 引入jar包
2. 配置`MqConstant`中连接`url`端口以及用户名密码等基本信息
3. 生产者(producer):同步发送消息:
`MqHelper.sendMessage(MqHelper.TEST, "test.mq.queue", content);`
4. 生产者(producer):异步发送消息:
`MqHelper.asyncSendMessage(MqHelper.TEST, "test.mq.queue", content);`
5. 消费者(consumer):消费消息较少的队列的消息或消息处理时间较长的消息:
```
SmsMqHelper.getSimpleMessage(MqHelper.TEST, "test.mq.queue", new MessageProcessor() {
            @Override
            public void processMessage(Message message) {
                String text = getStringFromTextMessage(message);
                logger.info("receive:{}", text);
            }
        });
```
6. 消费者(consumer):多线程消费消息量较大或消息处理耗时的消息:
```
SmsMqHelper.getMessage(MqHelper.TEST, "test.mq.queue", new MessageProcessor(10,100) {
            @Override
            public void processMessage(Message message) {
                 String text = getStringFromTextMessage(message);
                 logger.info("receive:{}", text);
            }
        });
```
其中,`MessageProcessor(10,100)`表示启用10个线程,缓冲100条消息!
ok,不用管了,在应用关闭时自动处理关闭,如果处理依赖spring bean,则需要在spring关闭时手动调用关闭.
如:实现`ApplicationListener<ContextClosedEvent>`的`onApplicationEvent`方法关闭!

Tip:使用相比spring更简单,且拓展性,个性化定制上面都更加灵活!