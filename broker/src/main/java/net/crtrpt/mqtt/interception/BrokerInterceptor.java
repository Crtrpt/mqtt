/*
 * Copyright (c) 2012-2018 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package net.crtrpt.mqtt.interception;

import net.crtrpt.mqtt.BrokerConstants;
import net.crtrpt.mqtt.broker.config.IConfig;
import net.crtrpt.mqtt.broker.subscriptions.Subscription;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.util.ReferenceCountUtil;
import net.crtrpt.mqtt.interception.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static net.crtrpt.mqtt.logging.LoggingUtils.getInterceptorIds;

/**
 * 异步拦截器
 */
public final class BrokerInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerInterceptor.class);
    private final Map<Class<?>, List<InterceptHandler>> handlers;
    /**
     * 线程池
     */
    private final ExecutorService executor;

    private BrokerInterceptor(int poolSize, List<InterceptHandler> handlers) {
        LOG.info("初始化拦截器. InterceptorIds={}", getInterceptorIds(handlers));
        this.handlers = new HashMap<>();
        for (Class<?> messageType : InterceptHandler.ALL_MESSAGE_TYPES) {
            this.handlers.put(messageType, new CopyOnWriteArrayList<InterceptHandler>());
        }
        for (InterceptHandler handler : handlers) {
            this.addInterceptHandler(handler);
        }
        executor = Executors.newFixedThreadPool(poolSize);
    }

    /**
     * Configures a broker interceptor, with a thread pool of one thread.
     *
     * @param handlers InterceptHandlers listeners.
     */
    public BrokerInterceptor(List<InterceptHandler> handlers) {
        this(1, handlers);
    }

    /**
     * Configures a broker interceptor using the pool size specified in the IConfig argument.
     * @param props configuration properties.
     * @param handlers InterceptHandlers listeners.
     *
     */
    public BrokerInterceptor(IConfig props, List<InterceptHandler> handlers) {
        this(Integer.parseInt(props.getProperty(BrokerConstants.BROKER_INTERCEPTOR_THREAD_POOL_SIZE, "1")), handlers);
    }

    /**
     * Shutdown graciously the executor service
     */
    void stop() {
        LOG.info("关闭线程池...");
        executor.shutdown();
        try {
            LOG.info("等待线程池关闭...");
            executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        if (!executor.isTerminated()) {
            LOG.warn("强行关闭中断线程池...");
            executor.shutdownNow();
        }
        LOG.info("interceptors stopped");
    }

    @Override
    public void notifyClientConnected(final MqttConnectMessage msg) {
        for (final InterceptHandler handler : this.handlers.get(InterceptConnectMessage.class)) {
            LOG.info("发送 MQTT CONNECT 消息给中断. CId={}, interceptorId={}",
                    msg.payload().clientIdentifier(), handler.getID());
            executor.execute(() -> handler.onConnect(new InterceptConnectMessage(msg)));
        }
    }

    @Override
    public void notifyClientDisconnected(final String clientID, final String username) {
        for (final InterceptHandler handler : this.handlers.get(InterceptDisconnectMessage.class)) {
            LOG.info("发送mqtt断开消息给中断. CId={}, username={}, interceptorId={}",
                clientID, username, handler.getID());
            executor.execute(() -> handler.onDisconnect(new InterceptDisconnectMessage(clientID, username)));
        }
    }

    @Override
    public void notifyClientConnectionLost(final String clientID, final String username) {
        for (final InterceptHandler handler : this.handlers.get(InterceptConnectionLostMessage.class)) {
            LOG.info("发送异常断开给中断 CId={}, username={}, " +
                "interceptorId={}", clientID, username, handler.getID());
            executor.execute(() -> handler.onConnectionLost(new InterceptConnectionLostMessage(clientID, username)));
        }
    }

    @Override
    public void notifyTopicPublished(final MqttPublishMessage msg, final String clientID, final String username) {
        msg.retain();

        executor.execute(() -> {
                try {
                    int messageId = msg.variableHeader().messageId();
                    String topic = msg.variableHeader().topicName();
                    for (InterceptHandler handler : handlers.get(InterceptPublishMessage.class)) {
                        LOG.info("通知mqtt发送消息给中断 CId={}, messageId={}, topic={}, "
                                + "interceptorId={}", clientID, messageId, topic, handler.getID());
                        handler.onPublish(new InterceptPublishMessage(msg, clientID, username));
                    }
                } finally {
                    ReferenceCountUtil.release(msg);
                }
        });
    }

    @Override
    public void notifyTopicSubscribed(final Subscription sub, final String username) {
        for (final InterceptHandler handler : this.handlers.get(InterceptSubscribeMessage.class)) {
            LOG.info("通知订阅消息给中断 CId={}, topicFilter={}, interceptorId={}",
                sub.getClientId(), sub.getTopicFilter(), handler.getID());
            executor.execute(() -> handler.onSubscribe(new InterceptSubscribeMessage(sub, username)));
        }
    }

    @Override
    public void notifyTopicUnsubscribed(final String topic, final String clientID, final String username) {
        for (final InterceptHandler handler : this.handlers.get(InterceptUnsubscribeMessage.class)) {
            LOG.info("取消订阅消息给中断. CId={}, topic={}, interceptorId={}", clientID,
                topic, handler.getID());
            executor.execute(() -> handler.onUnsubscribe(new InterceptUnsubscribeMessage(topic, clientID, username)));
        }
    }

    @Override
    public void notifyMessageAcknowledged(final InterceptAcknowledgedMessage msg) {
        for (final InterceptHandler handler : this.handlers.get(InterceptAcknowledgedMessage.class)) {
            LOG.info("通知 MQTT ACK 消息给中断. CId={}, messageId={}, topic={}, interceptorId={}",
                msg.getMsg()/*.getClientID()*/, msg.getPacketID(), msg.getTopic(), handler.getID());
            executor.execute(() -> handler.onMessageAcknowledged(msg));
        }
    }

    @Override
    public void addInterceptHandler(InterceptHandler interceptHandler) {
        Class<?>[] interceptedMessageTypes = getInterceptedMessageTypes(interceptHandler);
        LOG.info("增加中断处理器. InterceptorId={}, handledMessageTypes={}",
            interceptHandler.getID(), interceptedMessageTypes);
        for (Class<?> interceptMessageType : interceptedMessageTypes) {
            this.handlers.get(interceptMessageType).add(interceptHandler);
        }
    }

    @Override
    public void removeInterceptHandler(InterceptHandler interceptHandler) {
        Class<?>[] interceptedMessageTypes = getInterceptedMessageTypes(interceptHandler);
        LOG.info("移除中断处理器. InterceptorId={}, handledMessageTypes={}",
            interceptHandler.getID(), interceptedMessageTypes);
        for (Class<?> interceptMessageType : interceptedMessageTypes) {
            this.handlers.get(interceptMessageType).remove(interceptHandler);
        }
    }

    private static Class<?>[] getInterceptedMessageTypes(InterceptHandler interceptHandler) {
        Class<?>[] interceptedMessageTypes = interceptHandler.getInterceptedMessageTypes();
        if (interceptedMessageTypes == null) {
            return InterceptHandler.ALL_MESSAGE_TYPES;
        }
        return interceptedMessageTypes;
    }
}
