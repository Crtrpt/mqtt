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


import net.crtrpt.mqtt.interception.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异步拦截器
 */
public final class FunctionInterceptor implements InterceptHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionInterceptor.class);

    public FunctionInterceptor() {
        LOG.info("初始化function");
    }

    @Override
    public String getID() {
        return "1";
    }

    @Override
    public Class<?>[] getInterceptedMessageTypes() {
        return new Class[]{InterceptPublishMessage.class};
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        LOG.info("发布消息的时候执行 相关的操作");
    }
}
