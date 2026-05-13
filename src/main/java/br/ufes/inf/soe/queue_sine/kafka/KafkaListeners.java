package br.ufes.inf.soe.queue_sine.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListeners {

    private final Logger logger = LoggerFactory.getLogger(KafkaListeners.class);

    @KafkaListener(topics = "item-view-events", groupId = "queue-sine-group")
    public void handleItemView(ConsumerRecord<String, String> record) {
        logger.info("Received event topic={} key={} payload={}", record.topic(), record.key(), record.value());
        // TODO: handle telemetry item view event
    }

    @KafkaListener(topics = "click-stream-events", groupId = "queue-sine-group")
    public void handleClickStream(ConsumerRecord<String, String> record) {
        logger.info("Received event topic={} key={} payload={}", record.topic(), record.key(), record.value());
        // TODO: handle click stream event
    }

    @KafkaListener(topics = "cart-events", groupId = "queue-sine-group")
    public void handleCartEvent(ConsumerRecord<String, String> record) {
        logger.info("Received event topic={} key={} payload={}", record.topic(), record.key(), record.value());
        // TODO: handle cart event
    }

    @KafkaListener(topics = "order-status-events", groupId = "queue-sine-group")
    public void handleOrderStatusEvent(ConsumerRecord<String, String> record) {
        logger.info("Received event topic={} key={} payload={}", record.topic(), record.key(), record.value());
        // TODO: handle order status event
    }

    @KafkaListener(topics = "order", groupId = "queue-sine-group")
    public void handleOrderEvent(ConsumerRecord<String, String> record) {
        logger.info("Received event topic={} key={} payload={}", record.topic(), record.key(), record.value());
        // TODO: handle order creation event (product ids + client id)
    }
}
