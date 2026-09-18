package com.courier.dashboard.config;

import com.courier.dashboard.service.StatusUpdateListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

@Configuration
public class RedisPubSubConfig {

    private static final String CHANNEL = "courier:status-updates";

    /*
     * RedisMessageListenerContainer manages the subscription lifecycle -
     * connects to Redis, subscribes to channels, dispatches messages
     * to the correct listener, and handles reconnection automatically.
     *
     * It runs on a background thread - the dashboard's request threads
     * are not involved in receiving Pub/Sub messages.
     */
    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(
            RedisConnectionFactory connectionFactory,
            MessageListenerAdapter statusUpdateAdapter) {

        RedisMessageListenerContainer container =
                new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(
                statusUpdateAdapter,
                new ChannelTopic(CHANNEL));
        return container;
    }

    /*
     * MessageListenerAdapter wraps your listener class and handles
     * deserialization. It calls the method named "onMessage" by default,
     * or you specify the method name explicitly as below.
     *
     * "handleStatusUpdate" is the method on StatusUpdateListener
     * that will be called for each incoming message.
     */
    @Bean
    public MessageListenerAdapter statusUpdateAdapter(
            StatusUpdateListener statusUpdateListener) {
        return new MessageListenerAdapter(
                statusUpdateListener, "handleStatusUpdate");
    }
}