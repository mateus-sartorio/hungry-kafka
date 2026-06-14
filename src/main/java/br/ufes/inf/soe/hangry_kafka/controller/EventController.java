package br.ufes.inf.soe.hangry_kafka.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.ufes.inf.soe.hangry_kafka.config.TopicNames;
import br.ufes.inf.soe.hangry_kafka.dto.CartEvent;
import br.ufes.inf.soe.hangry_kafka.dto.CreateOrderRequest;
import br.ufes.inf.soe.hangry_kafka.dto.ItemViewEvent;
import br.ufes.inf.soe.hangry_kafka.dto.OrderStatusEvent;
import br.ufes.inf.soe.hangry_kafka.producer.EventProducer;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/events")
public class EventController {

    private final EventProducer eventProducer;

    public EventController(EventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }

    @PostMapping("/item-view")
    public ResponseEntity<Map<String, String>> itemView(@RequestBody ItemViewEvent event) {
        eventProducer.sendItemView(event);
        return accepted(TopicNames.ITEM_VIEW_EVENTS, String.valueOf(event.getClientId()));
    }

    @PostMapping("/cart")
    public ResponseEntity<Map<String, String>> cart(@RequestBody CartEvent event) {
        if (event.getEventId() == null) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getOccurredAt() == null) {
            event.setOccurredAt(Instant.now());
        }
        eventProducer.sendCart(event);
        return accepted(TopicNames.CART_EVENTS, event.getEventId());
    }

    @PostMapping("/order-status")
    public ResponseEntity<Map<String, String>> orderStatus(@RequestBody OrderStatusEvent event) {
        if (event.getEventId() == null) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getOccurredAt() == null) {
            event.setOccurredAt(Instant.now());
        }
        eventProducer.sendOrderStatus(event);
        return accepted(TopicNames.ORDER_STATUS_EVENTS, event.getEventId());
    }

    @PostMapping("/order")
    public ResponseEntity<Map<String, String>> order(@RequestBody CreateOrderRequest event) {
        String eventId = UUID.randomUUID().toString();
        eventProducer.sendOrder(event);
        return accepted(TopicNames.ORDER_EVENTS, eventId);
    }

    private ResponseEntity<Map<String, String>> accepted(String topic, String eventId) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("topic", topic, "eventId", eventId));
    }
}
