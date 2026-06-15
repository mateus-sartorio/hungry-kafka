package br.ufes.inf.soe.hungry_kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import br.ufes.inf.soe.hungry_kafka.config.TopicNames;
import br.ufes.inf.soe.hungry_kafka.dto.CartEvent;
import br.ufes.inf.soe.hungry_kafka.dto.CreateOrderRequest;
import br.ufes.inf.soe.hungry_kafka.dto.ItemViewEvent;
import br.ufes.inf.soe.hungry_kafka.dto.OrderStatusEvent;

@Service
public class EventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public EventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendItemView(ItemViewEvent event) {
        String key = event.getClientId() != null ? String.valueOf(event.getClientId()) : "unknown";
        kafkaTemplate.send(TopicNames.ITEM_VIEW_EVENTS, key, event);
    }

    public void sendCart(CartEvent event) {
        String key = event.getClientId() != null ? String.valueOf(event.getClientId()) : "unknown";
        kafkaTemplate.send(TopicNames.CART_EVENTS, key, event);
    }

    public void sendOrderStatus(OrderStatusEvent event) {
        kafkaTemplate.send(TopicNames.ORDER_STATUS_EVENTS, event.getOrderId(), event);
    }

    public void sendOrder(CreateOrderRequest event) {
        kafkaTemplate.send(TopicNames.ORDER_EVENTS, String.valueOf(event.getClientId()), event);
    }
}
