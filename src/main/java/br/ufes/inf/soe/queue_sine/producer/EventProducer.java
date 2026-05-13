package br.ufes.inf.soe.queue_sine.producer;

import br.ufes.inf.soe.queue_sine.config.TopicNames;
import br.ufes.inf.soe.queue_sine.dto.CartEvent;
import br.ufes.inf.soe.queue_sine.dto.ClickStreamEvent;
import br.ufes.inf.soe.queue_sine.dto.ItemViewEvent;
import br.ufes.inf.soe.queue_sine.dto.OrderEvent;
import br.ufes.inf.soe.queue_sine.dto.OrderStatusEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EventProducer {

    private final Logger logger = LoggerFactory.getLogger(EventProducer.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public EventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendItemView(ItemViewEvent event) {
        send(TopicNames.ITEM_VIEW_EVENTS, event.getUserId(), event);
    }

    public void sendClickStream(ClickStreamEvent event) {
        send(TopicNames.CLICK_STREAM_EVENTS, event.getItemId(), event);
    }

    public void sendCart(CartEvent event) {
        send(TopicNames.CART_EVENTS, event.getUserId(), event);
    }

    public void sendOrderStatus(OrderStatusEvent event) {
        send(TopicNames.ORDER_STATUS_EVENTS, event.getOrderId(), event);
    }

    public void sendOrder(OrderEvent event) {
        send(TopicNames.ORDER_EVENTS, event.getClientId(), event);
    }

    private void send(String topic, String key, Object event) {
        kafkaTemplate.send(topic, key, event);
        logger.info("Published event topic={} key={} payload={}", topic, key, event);
    }
}
