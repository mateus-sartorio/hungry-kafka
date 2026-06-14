package br.ufes.inf.soe.queue_sine.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic itemViewEventsTopic() {
        return TopicBuilder.name(TopicNames.ITEM_VIEW_EVENTS)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic cartEventsTopic() {
        return TopicBuilder.name(TopicNames.CART_EVENTS)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderStatusEventsTopic() {
        return TopicBuilder.name(TopicNames.ORDER_STATUS_EVENTS)
                .partitions(3)
                .replicas(2)
                .build();
    }

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(TopicNames.ORDER_EVENTS)
                .partitions(3)
                .replicas(2)
                .build();
    }

    @Bean
    public NewTopic hotItemEventsTopic() {
        return TopicBuilder.name(TopicNames.HOT_ITEM_EVENTS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderStatusChangedTopic() {
        return TopicBuilder.name(TopicNames.ORDER_STATUS_CHANGED)
                .partitions(3)
                .replicas(2)
                .build();
    }
}
