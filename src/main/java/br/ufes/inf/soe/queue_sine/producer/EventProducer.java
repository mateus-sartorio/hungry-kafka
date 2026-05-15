package br.ufes.inf.soe.queue_sine.producer;

import br.ufes.inf.soe.queue_sine.config.TopicNames;
import br.ufes.inf.soe.queue_sine.dto.CartEvent;
import br.ufes.inf.soe.queue_sine.dto.ClickStreamEvent;
import br.ufes.inf.soe.queue_sine.dto.ItemViewEvent;
import br.ufes.inf.soe.queue_sine.dto.CreateOrderRequest;
import br.ufes.inf.soe.queue_sine.dto.OrderStatusEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

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

    public void sendClickStream(ClickStreamEvent event) {
        kafkaTemplate.send(TopicNames.CLICK_STREAM_EVENTS, event.getItemId(), event);
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
