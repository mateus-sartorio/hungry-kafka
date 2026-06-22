package br.ufes.inf.soe.hungry_kafka.websocket;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import br.ufes.inf.soe.hungry_kafka.dto.AbandonedCartEvent;
import br.ufes.inf.soe.hungry_kafka.dto.HotItemEvent;
import br.ufes.inf.soe.hungry_kafka.dto.LeadItemEvent;

@Service
public class WebSocketService {

    private final SimpMessagingTemplate messagingTemplate;

    public WebSocketService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    public void sendOrderUpdate(Integer orderId, Object clientPayload, Object storePayload) {
        messagingTemplate.convertAndSend("/topic/orders/" + orderId, clientPayload);
        messagingTemplate.convertAndSend("/topic/orders", storePayload);
    }

    public void sendLeadItemAlert(LeadItemEvent event) {
        messagingTemplate.convertAndSend("/topic/lead-items", event);
    }

    public void sendHotItemAlert(HotItemEvent event) {
        // Broadcast destination: every client and the store subscribe to /topic/hot-items.
        messagingTemplate.convertAndSend("/topic/hot-items", event);
    }

    public void sendAbandonedCartAlert(AbandonedCartEvent event) {
        messagingTemplate.convertAndSend("/topic/abandoned-carts", event);
    }
}

