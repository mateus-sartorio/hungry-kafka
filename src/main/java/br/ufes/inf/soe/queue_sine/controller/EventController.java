package br.ufes.inf.soe.queue_sine.controller;

import br.ufes.inf.soe.queue_sine.config.TopicNames;
import br.ufes.inf.soe.queue_sine.dto.CartEvent;
import br.ufes.inf.soe.queue_sine.dto.ClickStreamEvent;
import br.ufes.inf.soe.queue_sine.dto.ItemViewEvent;
import br.ufes.inf.soe.queue_sine.dto.OrderEvent;
import br.ufes.inf.soe.queue_sine.dto.OrderStatusEvent;
import br.ufes.inf.soe.queue_sine.producer.EventProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
        if (event.getEventId() == null) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getObservedAt() == null) {
            event.setObservedAt(Instant.now());
        }
        eventProducer.sendItemView(event);
        return accepted(TopicNames.ITEM_VIEW_EVENTS, event.getEventId());
    }

    @PostMapping("/click-stream")
    public ResponseEntity<Map<String, String>> clickStream(@RequestBody ClickStreamEvent event) {
        if (event.getEventId() == null) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getClickedAt() == null) {
            event.setClickedAt(Instant.now());
        }
        if (event.getItemId() == null) {
            event.setItemId(event.getTargetId());
        }
        eventProducer.sendClickStream(event);
        return accepted(TopicNames.CLICK_STREAM_EVENTS, event.getEventId());
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
    public ResponseEntity<Map<String, String>> order(@RequestBody OrderEvent event) {
        if (event.getEventId() == null) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getOccurredAt() == null) {
            event.setOccurredAt(Instant.now());
        }
        eventProducer.sendOrder(event);
        return accepted(TopicNames.ORDER_EVENTS, event.getEventId());
    }

    private ResponseEntity<Map<String, String>> accepted(String topic, String eventId) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("topic", topic, "eventId", eventId));
    }
}
