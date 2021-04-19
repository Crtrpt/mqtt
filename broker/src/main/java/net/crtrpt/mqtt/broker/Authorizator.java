package net.crtrpt.mqtt.broker;

import net.crtrpt.mqtt.broker.subscriptions.Topic;
import net.crtrpt.mqtt.broker.security.IAuthorizatorPolicy;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static net.crtrpt.mqtt.broker.Utils.messageId;
import static io.netty.handler.codec.mqtt.MqttQoS.FAILURE;

final class Authorizator {

    private static final Logger LOG = LoggerFactory.getLogger(Authorizator.class);

    private final IAuthorizatorPolicy policy;

    Authorizator(IAuthorizatorPolicy policy) {
        this.policy = policy;
    }

    List<MqttTopicSubscription> verifyTopicsReadAccess(String clientID, String username, MqttSubscribeMessage msg) {
        List<MqttTopicSubscription> ackTopics = new ArrayList<>();

        final int messageId = messageId(msg);
        for (MqttTopicSubscription req : msg.payload().topicSubscriptions()) {
            Topic topic = new Topic(req.topicName());
            if (!policy.canRead(topic, username, clientID)) {
                LOG.warn("客户端没有读权限: {}, messageId: {}, " + "topic: {}", username, messageId, topic);
                ackTopics.add(new MqttTopicSubscription(topic.toString(), FAILURE));
            } else {
                MqttQoS qos;
                if (topic.isValid()) {
                    LOG.debug("订阅 username: {}, messageId: {}, topic: {}",
                        username, messageId, topic);
                    qos = req.qualityOfService();
                } else {
                    LOG.warn("Topic filter is not valid username: {}, messageId: {}, topic: {}",
                        username, messageId, topic);
                    qos = FAILURE;
                }
                ackTopics.add(new MqttTopicSubscription(topic.toString(), qos));
            }
        }
        return ackTopics;
    }

    boolean canWrite(Topic topic, String user, String client) {
        return policy.canWrite(topic, user, client);
    }

    boolean canRead(Topic topic, String user, String client) {
        return policy.canRead(topic, user, client);
    }
}
