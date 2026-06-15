package br.ufes.inf.soe.hangry_kafka.websocket;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;

import br.ufes.inf.soe.hangry_kafka.config.TopicNames;
import br.ufes.inf.soe.hangry_kafka.dto.CartEvent;
import br.ufes.inf.soe.hangry_kafka.dto.ItemViewEvent;
import br.ufes.inf.soe.hangry_kafka.dto.OrderStatusEvent;

@Controller
public class WebSocketEventController {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public WebSocketEventController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @MessageMapping("/item-view")
    public void handleItemView(ItemViewEvent event) {
        kafkaTemplate.send(TopicNames.ITEM_VIEW_EVENTS, String.valueOf(event.getProductId()), event);
    }

    @MessageMapping("/cart-event")
    public void handleCartEvent(CartEvent event) {
        kafkaTemplate.send(TopicNames.CART_EVENTS, String.valueOf(event.getProductId()), event);
    }

    @MessageMapping("/order-status")
    public void handleOrderStatusEvent(OrderStatusEvent event) {
        kafkaTemplate.send(TopicNames.ORDER_STATUS_EVENTS, event.getOrderId(), event);
    }
}