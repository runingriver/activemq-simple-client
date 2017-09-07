package com.activemq.simple.client;

import com.activemq.simple.client.config.MqConstant;
import org.slf4j.Logger;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 封装接收消息的业务逻辑,用于处理消息较多场景,使用者需继承该类,并实现processMessage,处理具体的业务逻辑!
 * 提供多线程处理消息.默认10个线程处理,false不使用线程池!
 * Tip：线程池会保存1000条消息,确保消息必须能处理.
 */
public abstract class MessageProcessor implements MessageListener {
    private static final Logger logger = MqConstant.LOG;

    private static final AtomicInteger poolNumber = new AtomicInteger(0);
    private static final int THREAD_QUEUE_CAPACITY = 1000;

    private ThreadPoolExecutor service;
    private Thread shutdownHook;
    private boolean isUsedThreadPool = true;
    private Semaphore shutdownSemaphore;

    /**
     * 处理MQ消息逻辑实现
     *
     * @param message MQ消息
     */
    public abstract void processMessage(Message message);


    /**
     * 使用默认线程池,最大10个线程,线程队列最多保存1000条消息
     */
    public MessageProcessor() {
        this(10);
    }

    /**
     * 使用默认线程池,指定最大线程数,线程队列保存消息数
     *
     * @param nThread 指定最大线程数
     */
    public MessageProcessor(int nThread) {
        this(nThread, THREAD_QUEUE_CAPACITY);
    }

    /**
     * 使用默认线程池,指定最大线程数,线程队列保存消息数
     *
     * @param nThread   指定最大线程数
     * @param queueSize 线程队列保存消息数
     */
    public MessageProcessor(int nThread, int queueSize) {
        //自定义线程池,默认,最大nThread个,队列保存THREAD_QUEUE_CAPACITY个,再来则交给Session线程自己处理.
        service = new ThreadPoolExecutor(nThread, nThread, 0L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(queueSize),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        logger.info("create thread,current thread:{},poolNumber:{}", Thread.currentThread().getName(), poolNumber.get());
                        return new Thread(r, "MQConsumer-Thread-" + poolNumber.incrementAndGet());
                    }
                }, new ThreadPoolExecutor.CallerRunsPolicy());

        registerShutdownHook();
    }

    /**
     * 允许使用自定义的线程池
     *
     * @param threadPool 自定义的线程池
     */
    public MessageProcessor(ThreadPoolExecutor threadPool) {
        if (threadPool == null || threadPool.isTerminated()) {
            throw new RuntimeException("parameter of threadPool is null or isTerminated.");
        }
        service = threadPool;
        registerShutdownHook();
    }

    /**
     * 不使用线程池,false即可
     *
     * @param isUsedThreadPool false-不使用线程池
     */
    public MessageProcessor(boolean isUsedThreadPool) {
        this.isUsedThreadPool = false;
    }

    @Override
    public void onMessage(final Message message) {
        if (isUsedThreadPool) {
            service.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        processMessage(message);
                    } catch (Throwable e) {
                        //线程池处理,catch住
                        logger.error("process message exception.message:{}", message.toString());
                    }
                }
            });
        } else {
            try {
                processMessage(message);
            } catch (Throwable e) {
                logger.error("process message exception.message:{}", message.toString());
            }
        }
    }

    /**
     * 关闭线程池,如果是钩子线程关闭,则等到钩子关闭完毕.
     */
    public void close() {
        if (isUsedThreadPool) {
            try {
                shutdownSemaphore.acquire();
                if (service.isTerminating() || service.isShutdown() || service.isTerminated()) {
                    return;
                }
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
                logger.info("close MessageProcessor,{} task retain in the queue", service.getQueue().size());
                closeThreadPool();
                logger.info("close MessageProcessor finish,isTerminated:{}", service.isTerminated());
            } catch (InterruptedException e) {
                logger.error("shutdown semaphore acquire exception.", e);
            } finally {
                shutdownSemaphore.release();
                logger.info("close method release semaphore.");
            }
        }
    }

    private void closeThreadPool() {
        if (service != null && !service.isShutdown()) {
            service.shutdown();
            try {
                service.awaitTermination(1, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                logger.error("await pooled task execute finish error.", e);
            }
        }
    }

    /**
     * 注册钩子,当使用线程池时,将队列中未执行完的消息,执行完
     * hook可能会在spring注销后执行
     */
    public void registerShutdownHook() {
        shutdownHook = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    shutdownSemaphore.acquire();
                    if (service.isTerminating() || service.isShutdown() || service.isTerminated()) {
                        return;
                    }
                    logger.info("shutdown hook,{} task retain in the queue", service.getQueue().size());
                    closeThreadPool();
                    logger.info("close pool finish,isTerminated:{}", service.isTerminated());
                } catch (InterruptedException e) {
                    logger.error("shutdown semaphore acquire exception.", e);
                } finally {
                    shutdownSemaphore.release();
                    logger.info("shutdown hook release semaphore.");
                }
            }
        }, "MessageProcessor-shutdown-hook-thread");

        Runtime.getRuntime().addShutdownHook(shutdownHook);
        shutdownSemaphore = new Semaphore(1);
    }

    protected String getStringFromTextMessage(Message message) {
        String text = "";
        if (message instanceof TextMessage) {
            try {
                text = ((TextMessage) message).getText();
            } catch (JMSException e) {
                logger.error("transfer message to TextMessage exception.", e);
            }
        }
        return text;
    }

    protected byte[] getByteArrayFromByteMessage(Message message) {
        byte[] bytes = null;
        if (message instanceof BytesMessage) {
            try {
                int bodyLength = (int) ((BytesMessage) message).getBodyLength();
                bytes = new byte[bodyLength];
                ((BytesMessage) message).readBytes(bytes);
            } catch (JMSException e) {
                logger.error("transfer message to BytesMessage exception.", e);
            }

        }
        return bytes;
    }

    protected Map<String, Object> getMapFromMapMessage(Message message) {
        Map<String, Object> map = new HashMap<String, Object>();
        if (message instanceof MapMessage) {
            MapMessage mapMessage = ((MapMessage) message);
            try {
                Enumeration<String> en = mapMessage.getMapNames();
                while (en.hasMoreElements()) {
                    String key = en.nextElement();
                    map.put(key, mapMessage.getObject(key));
                }
            } catch (JMSException e) {
                logger.error("process MapMessage exception.", e);
            }
        }
        return map;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(200);
        builder.append("MessageProcessor{");
        builder.append("isUsedThreadPool=").append(isUsedThreadPool);
        builder.append(",poolNumber=").append(poolNumber.get());
        if (isUsedThreadPool) {
            builder.append(",service: corePoolSize=").append(service.getCorePoolSize());
            builder.append(",maxPoolSize=").append(service.getMaximumPoolSize());
            builder.append(",taskCount=").append(service.getTaskCount());
            builder.append(",CompleteTaskCount=").append(service.getCompletedTaskCount());
            builder.append(",isTerminated=").append(service.isTerminated());
            builder.append(",rejectStrategy=").append(service.getRejectedExecutionHandler());
            builder.append(",activeThread=").append(service.getActiveCount());
            builder.append(",queue size=").append(service.getQueue().size());
        }
        builder.append("}");
        return builder.toString();
    }
}
