package br.ufes.inf.soe.queue_sine.producer;

import br.ufes.inf.soe.queue_sine.config.TopicNames;
import br.ufes.inf.soe.queue_sine.dto.CartEvent;
import br.ufes.inf.soe.queue_sine.dto.ClickStreamEvent;
import br.ufes.inf.soe.queue_sine.dto.ItemViewEvent;
import br.ufes.inf.soe.queue_sine.dto.OrderStatusEvent;
import org.springframework.stereotype.Service;

@Service
public class EventProducer {

    public EventProducer() {
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

    private void send(String topic, String key, Object event) {
        // TODO: Implement sending logic using KafkaTemplate
    }
}
