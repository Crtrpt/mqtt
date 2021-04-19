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
package net.crtrpt.mqtt.broker;


import net.crtrpt.mqtt.BrokerConstants;
import net.crtrpt.mqtt.broker.subscriptions.Topic;
import net.crtrpt.mqtt.interception.InterceptHandler;
import net.crtrpt.mqtt.MemorySubscriptionsRepository;
import net.crtrpt.mqtt.interception.BrokerInterceptor;
import net.crtrpt.mqtt.broker.subscriptions.CTrieSubscriptionDirectory;
import net.crtrpt.mqtt.broker.subscriptions.ISubscriptionsDirectory;
import net.crtrpt.mqtt.broker.security.IAuthenticator;
import net.crtrpt.mqtt.broker.security.IAuthorizatorPolicy;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import net.crtrpt.mqtt.broker.config.IConfig;
import net.crtrpt.mqtt.broker.config.MemoryConfig;
import net.crtrpt.mqtt.logging.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Server {
    private static final Logger log = LoggerFactory.getLogger(Server.class);
    private ScheduledExecutorService scheduler;
    private NewNettyAcceptor acceptor;
    private volatile boolean initialized;
    private PostOffice dispatcher;
    private BrokerInterceptor interceptor;
    private SessionRegistry sessions;

    public static void main(String[] args) throws IOException {
        final Server server = new Server();
        server.startServer();
        Runtime.getRuntime().addShutdownHook(new Thread(server::stopServer));
    }


    public void startServer() throws IOException {
        startServer(new MemoryConfig(new Properties()));
    }


    public void startServer(Properties configProps) throws IOException {
        log.info("Starting Moquette integration using properties object");
        final IConfig config = new MemoryConfig(configProps);
        startServer(config);
    }

    /**
     * Starts Moquette bringing the configuration files from the given Config implementation.
     *
     * @param config the configuration to use to start the broker.
     * @throws IOException in case of any IO Error.
     */
    public void startServer(IConfig config) throws IOException {
        log.info("Starting Moquette integration using IConfig instance");
        startServer(config, null);
    }

    public void startServer(IConfig config, List<? extends InterceptHandler> handlers) throws IOException {
        startServer(config, handlers, null, (clientId, username, password) -> {
            log.info("判断用户名称密码是否正确");
            return true;
        }, new IAuthorizatorPolicy() {
            @Override
            public boolean canWrite(Topic topic, String user, String client) {
                log.info("判断是否可写 默认全部可以");
                return true;
            }

            @Override
            public boolean canRead(Topic topic, String user, String client) {
                log.info("判断是否可读 默认全部可以");
                return true;
            }
        });
    }

    public void startServer(IConfig config, List<? extends InterceptHandler> handlers, ISslContextCreator sslCtxCreator,
                            IAuthenticator authenticator, IAuthorizatorPolicy authorizatorPolicy) {
        final long start = System.currentTimeMillis();
        if (handlers == null) {
            handlers = Collections.emptyList();
        }
        log.info("Starting Moquette Server. MQTT message interceptors={}", LoggingUtils.getInterceptorIds(handlers));

        scheduler = Executors.newScheduledThreadPool(1);

        final String handlerProp = System.getProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME);
        if (handlerProp != null) {
            config.setProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME, handlerProp);
        }
        final String persistencePath = config.getProperty(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME);
        log.info("Configuring Using persistent store file, path: {}", persistencePath);
        initInterceptors(config, handlers);
        log.info("初始化协议处理器");
        if (sslCtxCreator == null) {
            log.info("Using default SSL context creator");
            sslCtxCreator = new DefaultMoquetteSslContextCreator(config);
        }
        authenticator = initializeAuthenticator(authenticator, config);
        authorizatorPolicy = initializeAuthorizatorPolicy(authorizatorPolicy, config);

        final ISubscriptionsRepository subscriptionsRepository;
        final IQueueRepository queueRepository;
        final IRetainedRepository retainedRepository;

        log.info("配置内存订阅存储");
        //TODO 使用 二级缓存来处理
        subscriptionsRepository = new MemorySubscriptionsRepository();
        queueRepository = new MemoryQueueRepository();
        retainedRepository = new MemoryRetainedRepository();

        ISubscriptionsDirectory subscriptions = new CTrieSubscriptionDirectory();
        subscriptions.init(subscriptionsRepository);
        final Authorizator authorizator = new Authorizator(authorizatorPolicy);
        sessions = new SessionRegistry(subscriptions, queueRepository, authorizator);
        dispatcher = new PostOffice(subscriptions, retainedRepository, sessions, interceptor, authorizator);
        final BrokerConfiguration brokerConfig = new BrokerConfiguration(config);
        MQTTConnectionFactory connectionFactory = new MQTTConnectionFactory(brokerConfig, authenticator, sessions,
            dispatcher);

        final NewNettyMQTTHandler mqttHandler = new NewNettyMQTTHandler(connectionFactory);
        acceptor = new NewNettyAcceptor();
        acceptor.initialize(mqttHandler, config, sslCtxCreator);

        final long startTime = System.currentTimeMillis() - start;
        log.info("moquette 启动成功 {} ms", startTime);
        initialized = true;
    }

    private IAuthorizatorPolicy initializeAuthorizatorPolicy(IAuthorizatorPolicy authorizatorPolicy, IConfig props) {
        log.info("配置授权策略");
        return authorizatorPolicy;
    }

    private IAuthenticator initializeAuthenticator(IAuthenticator authenticator, IConfig props) {
        log.info("配置MQTT认证");
        return authenticator;
    }

    private void initInterceptors(IConfig props, List<? extends InterceptHandler> embeddedObservers) {
        log.info("初始化消息中断处理器");
        List<InterceptHandler> observers = new ArrayList<>(embeddedObservers);
        String interceptorClassName = props.getProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME);
        if (interceptorClassName != null && !interceptorClassName.isEmpty()) {
            InterceptHandler handler = loadClass(interceptorClassName, InterceptHandler.class,
                Server.class, this);
            if (handler != null) {
                observers.add(handler);
            }
        }
        interceptor = new BrokerInterceptor(props, observers);
    }

    @SuppressWarnings("unchecked")
    private <T, U> T loadClass(String className, Class<T> intrface, Class<U> constructorArgClass, U props) {
        T instance = null;
        try {
            // check if constructor with constructor arg class parameter
            // exists
            log.info("Invoking constructor with {} argument. ClassName={}, interfaceName={}",
                constructorArgClass.getName(), className, intrface.getName());
            instance = this.getClass().getClassLoader()
                .loadClass(className)
                .asSubclass(intrface)
                .getConstructor(constructorArgClass)
                .newInstance(props);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
            log.info("Unable to invoke constructor with {} argument. ClassName={}, interfaceName={}, cause={}, " +
                    "errorMessage={}", constructorArgClass.getName(), className, intrface.getName(), ex.getCause(),
                ex.getMessage());
            return null;
        } catch (NoSuchMethodException | InvocationTargetException e) {
            try {
                log.info("Invoking default constructor. ClassName={}, interfaceName={}", className, intrface.getName());
                // fallback to default constructor
                instance = this.getClass().getClassLoader()
                    .loadClass(className)
                    .asSubclass(intrface)
                    .getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException |
                NoSuchMethodException | InvocationTargetException ex) {
                log.info("Unable to invoke default constructor. ClassName={}, interfaceName={}, cause={}, " +
                    "errorMessage={}", className, intrface.getName(), ex.getCause(), ex.getMessage());
                return null;
            }
        }

        return instance;
    }

    /**
     * Use the broker to publish a message. It's intended for embedding applications. It can be used
     * only after the integration is correctly started with startServer.
     *
     * @param msg      the message to forward.
     * @param clientId the id of the sending integration.
     * @throws IllegalStateException if the integration is not yet started
     */
    public void internalPublish(MqttPublishMessage msg, final String clientId) {
        final int messageID = msg.variableHeader().packetId();
        if (!initialized) {
            log.info("Moquette is not started, internal message cannot be published. CId: {}, messageId: {}", clientId,
                messageID);
            throw new IllegalStateException("Can't publish on a integration is not yet started");
        }
        log.info("Internal publishing message CId: {}, messageId: {}", clientId, messageID);
        dispatcher.internalPublish(msg);
    }

    public void stopServer() {
        log.info("Unbinding integration from the configured ports");
        acceptor.close();
        log.info("Stopping MQTT protocol processor");
        initialized = false;

        // calling shutdown() does not actually stop tasks that are not cancelled,
        // and SessionsRepository does not stop its tasks. Thus shutdownNow().
        scheduler.shutdownNow();


        log.info("Moquette integration has been stopped.");
    }

    public int getPort() {
        return acceptor.getPort();
    }


    /**
     * 动态增加处理器
     *
     * @param interceptHandler the handler to add.
     */
    public void addInterceptHandler(InterceptHandler interceptHandler) {
        if (!initialized) {
            log.info("Moquette is not started, MQTT message interceptor cannot be added. InterceptorId={}",
                interceptHandler.getID());
            throw new IllegalStateException("Can't register interceptors on a integration that is not yet started");
        }
        log.info("Adding MQTT message interceptor. InterceptorId={}", interceptHandler.getID());
        interceptor.addInterceptHandler(interceptHandler);
    }

    /**
     * 动态删除处理器
     *
     * @param interceptHandler the handler to remove.
     */
    public void removeInterceptHandler(InterceptHandler interceptHandler) {
        if (!initialized) {
            log.info("Moquette is not started, MQTT message interceptor cannot be removed. InterceptorId={}",
                interceptHandler.getID());
            throw new IllegalStateException("Can't deregister interceptors from a integration that is not yet started");
        }
        log.info("Removing MQTT message interceptor. InterceptorId={}", interceptHandler.getID());
        interceptor.removeInterceptHandler(interceptHandler);
    }

    /**
     * 获取全部的客户端
     */
    public Collection<ClientDescriptor> listConnectedClients() {
        return sessions.listConnectedClients();
    }
}
