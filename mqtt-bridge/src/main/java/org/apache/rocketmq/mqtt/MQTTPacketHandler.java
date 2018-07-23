/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.mqtt;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.mqtt.util.MessageUtil.getMqttConnackMessage;
import static org.apache.rocketmq.mqtt.util.MessageUtil.getMqttPingrespMessage;
import static org.apache.rocketmq.mqtt.util.MessageUtil.getMqttPubackMessage;
import static org.apache.rocketmq.mqtt.util.MessageUtil.getMqttPubcompMessage;
import static org.apache.rocketmq.mqtt.util.MessageUtil.getMqttPublishMessage;
import static org.apache.rocketmq.mqtt.util.MessageUtil.getMqttPubrecMessage;
import static org.apache.rocketmq.mqtt.util.MessageUtil.getMqttPubrelMessage;
import static org.apache.rocketmq.mqtt.util.MessageUtil.getMqttSubackMessage;
import static org.apache.rocketmq.mqtt.util.MessageUtil.getMqttUnsubackMessage;
import static org.apache.rocketmq.mqtt.util.MessageUtil.getRocketmqMessage;

public class MQTTPacketHandler extends ChannelInboundHandlerAdapter {

    private static final int MAX_PACKET_SIZE = 1024 * 1024;
    private DefaultMQProducer producer;
    private DefaultMQPushConsumer consumer;
    private MqttMessage mqttMessage;
    private String clientId;

    private List<MqttTopicSubscription> subscriptions;
    private ChannelHandlerContext ctx;

    private Logger log = LoggerFactory.getLogger(MQTTPacketHandler.class);

    public MQTTPacketHandler() {
    }

    public void channelRead(ChannelHandlerContext context, Object msg) {
        if (msg instanceof MqttMessage) {
            mqttMessage = (MqttMessage) msg;
            switch (mqttMessage.fixedHeader().messageType()) {
                case CONNECT:
                    ctx = context;
                    try {
                        onMqttConnectMessage(mqttMessage);
                    } catch (MQClientException e) {
                        e.printStackTrace();
                    }
                    break;
                case DISCONNECT:
                    onMqttDisconnectMessage(mqttMessage);
                    break;
                case SUBSCRIBE:
                    onMqttSubscribeMessage(mqttMessage);
                    break;
                case UNSUBSCRIBE:
                    onMqttUnsubscribeMessage(mqttMessage);
                    break;
                case PUBLISH:
                    onMqttPublishMessage(mqttMessage);
                    break;
                case PUBACK:
                    onMqttPubackMessage(mqttMessage);
                    break;
                case PUBREC:
                    onMqttPubrecMessage(mqttMessage);
                    break;
                case PUBREL:
                    onMqttPubrelMessage(mqttMessage);
                    break;
                case PUBCOMP:
                    onMqttPubcompMessage(mqttMessage);
                    break;
                case PINGREQ:
                    onMqttPingreqMessage(mqttMessage);
                    break;
            }
        }

    }

    private void onMqttSubscribeMessage(MqttMessage message) {
        if (consumer == null) {
            try {
                initConsumer(clientId);
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        }
        MqttSubscribeMessage subscribeMessage = (MqttSubscribeMessage) message;
        subscriptions = subscribeMessage.payload().topicSubscriptions();
        subscriptions.forEach(subscription -> {
            try {
                consumer.subscribe(subscription.topicName(), "*");
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        });
        sendToMqtt(getMqttSubackMessage((MqttSubscribeMessage) message));
    }

    private void onMqttDisconnectMessage(MqttMessage message) {
        doDisconnect();
    }

    private void initConsumer(String groupName) throws MQClientException {
        consumer = new DefaultMQPushConsumer(groupName);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setPullInterval(0);
        consumer.setMessageListener((MessageListenerOrderly) (msgs, context) -> {
            msgs.forEach(msg -> sendToMqtt(getMqttPublishMessage(msg, ctx.alloc().buffer(), false)));
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();
    }

    private void initProducer(String groupName) throws MQClientException {
        producer = new DefaultMQProducer("mqtt-producer-" + clientId);
        producer.setRetryTimesWhenSendAsyncFailed(0);
        producer.start();
    }

    private void onMqttConnectMessage(MqttMessage message) throws MQClientException {
        MqttConnectMessage connectMessage = (MqttConnectMessage) message;
        if (isFirstConnectMessage(connectMessage)) {
            clientId = connectMessage.payload().clientIdentifier();
            sendToMqtt(getMqttConnackMessage((MqttConnectMessage) message));
            return;
        }
        doDisconnect();
    }

    private void onMqttUnsubscribeMessage(MqttMessage message) {
        subscriptions = null;
        sendToMqtt(getMqttUnsubackMessage((MqttUnsubscribeMessage) message));
    }

    private void onMqttPublishMessage(MqttMessage message) {
        MqttPublishMessage publishMessage = (MqttPublishMessage) message;

        if (!publishMessage.fixedHeader().isDup()) {
            sendToRocketMQ(getRocketmqMessage(publishMessage));
        }

        switch (publishMessage.fixedHeader().qosLevel()) {
            case AT_MOST_ONCE:
                doDisconnect();
                break;
            case AT_LEAST_ONCE:
                sendToMqtt(getMqttPubackMessage(publishMessage));
                break;
            case EXACTLY_ONCE:
                sendToMqtt(getMqttPubrecMessage(publishMessage));
                break;
        }
    }

    private void onMqttPubackMessage(MqttMessage message) {
        /* commit the message */
    }

    private void onMqttPubrecMessage(MqttMessage message) {
        /* send Pubrel to client */
        sendToMqtt(getMqttPubrelMessage(message));
    }

    private void onMqttPubrelMessage(MqttMessage message) {
        sendToMqtt(getMqttPubcompMessage(message));
    }

    private void onMqttPubcompMessage(MqttMessage message) {
        /* push next message to client */
    }

    private void onMqttPingreqMessage(MqttMessage message) {
        sendToMqtt(getMqttPingrespMessage(message));
    }

    private boolean isFirstConnectMessage(MqttConnectMessage message) {
        return clientId == null || message.variableHeader().isCleanSession();
    }

    private void sendToMqtt(MqttMessage message) {
        ctx.writeAndFlush(message);
    }

    private void sendToRocketMQ(Message rocketmqMessage) {
        if (producer == null) {
            try {
                initProducer("mqtt-producer-" + clientId);
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        }
        try {
            producer.send(rocketmqMessage, new SendCallback() {
                @Override public void onSuccess(SendResult sendResult) {
                    if (log.isDebugEnabled()) {
                        log.debug("send message id = {}", sendResult.getMsgId());
                    }
                }

                @Override public void onException(Throwable e) {
                    e.printStackTrace();
                }
            });
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void doDisconnect() {
        /* QoS level 0*/
        ctx.disconnect();
        clientId = null;
        subscriptions = null;
        /* TODO complete QoS level 1 and 2 */
        /* QoS level 1*/
        /* QoS level 2*/
    }

}
