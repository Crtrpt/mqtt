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
package net.crtrpt.mqtt;

import net.crtrpt.mqtt.broker.ISubscriptionsRepository;
import net.crtrpt.mqtt.broker.subscriptions.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MemorySubscriptionsRepository implements ISubscriptionsRepository {

    private static final Logger log = LoggerFactory.getLogger(MemorySubscriptionsRepository.class);

    private final List<Subscription> subscriptions = new ArrayList<>();

    @Override
    public List<Subscription> listAllSubscriptions() {
        return Collections.unmodifiableList(subscriptions);
    }

    @Override
    public void addNewSubscription(Subscription subscription) {
        log.info("增加新的订阅 clientId:{} topic{}",subscription.getClientId(),subscription.getTopicFilter());
        subscriptions.add(subscription);
    }

    @Override
    public void removeSubscription(String topic, String clientID) {
        log.info("取消订阅 clientId{} topic{} ",clientID,topic);
        subscriptions.stream()
            .filter(s -> s.getTopicFilter().toString().equals(topic) && s.getClientId().equals(clientID))
            .findFirst()
            .ifPresent(subscriptions::remove);
    }
}
