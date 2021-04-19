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
import net.crtrpt.mqtt.broker.config.IConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import net.crtrpt.mqtt.broker.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static net.crtrpt.mqtt.BrokerConstants.*;
import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;

class NewNettyAcceptor {

    public static final String PLAIN_MQTT_PROTO = "TCP MQTT";

    private abstract static class PipelineInitializer {
        abstract void init(SocketChannel channel) throws Exception;
    }

    private class LocalPortReaderFutureListener implements ChannelFutureListener {
        private String transportName;

        LocalPortReaderFutureListener(String transportName) {
            this.transportName = transportName;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
                final SocketAddress localAddress = future.channel().localAddress();
                if (localAddress instanceof InetSocketAddress) {
                    InetSocketAddress inetAddress = (InetSocketAddress) localAddress;
                    LOG.info("bound {} port: {}", transportName, inetAddress.getPort());
                    int port = inetAddress.getPort();
                    ports.put(transportName, port);
                }
            }
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(NewNettyAcceptor.class);

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final Map<String, Integer> ports = new HashMap<>();
    private BytesMetricsCollector bytesMetricsCollector = new BytesMetricsCollector();
    private MessageMetricsCollector metricsCollector = new MessageMetricsCollector();
    private Optional<? extends ChannelInboundHandler> metrics;

    private int nettySoBacklog;
    private boolean nettySoReuseaddr;
    private boolean nettyTcpNodelay;
    private boolean nettySoKeepalive;
    private int nettyChannelTimeoutSeconds;
    private int maxBytesInMessage;

    private Class<? extends ServerSocketChannel> channelClass;

    public void initialize(NewNettyMQTTHandler mqttHandler, IConfig props, ISslContextCreator sslCtxCreator) {
        LOG.info("初始化");

        nettySoBacklog = props.intProp(BrokerConstants.NETTY_SO_BACKLOG_PROPERTY_NAME, 128);
        nettySoReuseaddr = props.boolProp(BrokerConstants.NETTY_SO_REUSEADDR_PROPERTY_NAME, true);
        nettyTcpNodelay = props.boolProp(BrokerConstants.NETTY_TCP_NODELAY_PROPERTY_NAME, true);
        nettySoKeepalive = props.boolProp(BrokerConstants.NETTY_SO_KEEPALIVE_PROPERTY_NAME, true);
        nettyChannelTimeoutSeconds = props.intProp(BrokerConstants.NETTY_CHANNEL_TIMEOUT_SECONDS_PROPERTY_NAME, 10);
        maxBytesInMessage = props.intProp(BrokerConstants.NETTY_MAX_BYTES_PROPERTY_NAME,
                BrokerConstants.DEFAULT_NETTY_MAX_BYTES_IN_MESSAGE);

        boolean epoll = props.boolProp(BrokerConstants.NETTY_EPOLL_PROPERTY_NAME, false);
        if (epoll) {
            LOG.info("Netty is using Epoll");
            bossGroup = new EpollEventLoopGroup();
            workerGroup = new EpollEventLoopGroup();
            channelClass = EpollServerSocketChannel.class;
        } else {
            LOG.info("windows 平台使用NIO");
            bossGroup = new NioEventLoopGroup();
            workerGroup = new NioEventLoopGroup();
            channelClass = NioServerSocketChannel.class;
        }

        initializePlainTCPTransport(mqttHandler, props);
    }

    private void initFactory(String host, int port, String protocol, final PipelineInitializer pipelieInitializer) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup).channel(channelClass)
                .childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        pipelieInitializer.init(ch);
                    }
                })
                .option(ChannelOption.SO_BACKLOG, nettySoBacklog)
                .option(ChannelOption.SO_REUSEADDR, nettySoReuseaddr)
                .childOption(ChannelOption.TCP_NODELAY, nettyTcpNodelay)
                .childOption(ChannelOption.SO_KEEPALIVE, nettySoKeepalive);
        try {
            ChannelFuture f = b.bind(host, port);
            LOG.info("服务已经启动 host={}, port={}, protocol={}", host, port, protocol);
            f.sync()
                .addListener(new LocalPortReaderFutureListener(protocol))
                .addListener(FIRE_EXCEPTION_ON_FAILURE);
        } catch (Exception ex) {
            if (ex instanceof BindException) {
               LOG.error("无法绑定端口: " + port, ex);
               System.exit(1);
            } else {
                LOG.error("An interruptedException was caught while initializing integration. Protocol={}", protocol, ex);
                throw new RuntimeException(ex);
            }
        }
    }

    public int getPort() {
        return ports.computeIfAbsent(PLAIN_MQTT_PROTO, i -> 0);
    }

    private void initializePlainTCPTransport(NewNettyMQTTHandler handler, IConfig props) {
        LOG.info("配置 tcp mqtt ");
        final MqttIdleTimeoutHandler timeoutHandler = new MqttIdleTimeoutHandler();
        String host = props.getProperty(BrokerConstants.HOST_PROPERTY_NAME);
        String tcpPortProp = props.getProperty(PORT_PROPERTY_NAME, DISABLED_PORT_BIND);
        int port = Integer.parseInt(tcpPortProp);
        initFactory(host, port, PLAIN_MQTT_PROTO, new PipelineInitializer() {
            @Override
            void init(SocketChannel channel) {
                ChannelPipeline pipeline = channel.pipeline();
                configureMQTTPipeline(pipeline, timeoutHandler, handler);
            }
        });
    }

    private void configureMQTTPipeline(ChannelPipeline pipeline, MqttIdleTimeoutHandler timeoutHandler,
                                       NewNettyMQTTHandler handler) {
        pipeline.addFirst("idleStateHandler", new IdleStateHandler(nettyChannelTimeoutSeconds, 0, 0));
        pipeline.addAfter("idleStateHandler", "idleEventHandler", timeoutHandler);
        pipeline.addLast("logger", new LoggingHandler("Netty", LogLevel.ERROR));
        pipeline.addFirst("bytemetrics", new BytesMetricsHandler(bytesMetricsCollector));
        pipeline.addLast("autoflush", new AutoFlushHandler(1, TimeUnit.SECONDS));
        pipeline.addLast("decoder", new MqttDecoder(maxBytesInMessage));
        pipeline.addLast("encoder", MqttEncoder.INSTANCE);
        pipeline.addLast("metrics", new MessageMetricsHandler(metricsCollector));
        pipeline.addLast("messageLogger", new MQTTMessageLogger());
        pipeline.addLast("handler", handler);
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    public void close() {
        LOG.info("Closing Netty acceptor...");
        if (workerGroup == null || bossGroup == null) {
            LOG.error("Netty acceptor is not initialized");
            throw new IllegalStateException("Invoked close on an Acceptor that wasn't initialized");
        }
        Future<?> workerWaiter = workerGroup.shutdownGracefully();
        Future<?> bossWaiter = bossGroup.shutdownGracefully();

        LOG.info("Waiting for worker and boss event loop groups to terminate...");
        try {
            workerWaiter.await(10, TimeUnit.SECONDS);
            bossWaiter.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException iex) {
            LOG.warn("An InterruptedException was caught while waiting for event loops to terminate...");
        }

        if (!workerGroup.isTerminated()) {
            LOG.warn("Forcing shutdown of worker event loop...");
            workerGroup.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS);
        }

        if (!bossGroup.isTerminated()) {
            LOG.warn("Forcing shutdown of boss event loop...");
            bossGroup.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS);
        }

        MessageMetrics metrics = metricsCollector.computeMetrics();
        BytesMetrics bytesMetrics = bytesMetricsCollector.computeMetrics();
        LOG.info("Metrics messages[read={}, write={}] bytes[read={}, write={}]", metrics.messagesRead(),
                 metrics.messagesWrote(), bytesMetrics.readBytes(), bytesMetrics.wroteBytes());
    }

}
