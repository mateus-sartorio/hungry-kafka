package br.ufes.inf.soe.hungry_kafka.websocket;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import br.ufes.inf.soe.hungry_kafka.dto.AbandonedCartEvent;
import br.ufes.inf.soe.hungry_kafka.dto.ClientOrderResponse;
import br.ufes.inf.soe.hungry_kafka.dto.HotItemEvent;
import br.ufes.inf.soe.hungry_kafka.dto.LeadItemEvent;
import br.ufes.inf.soe.hungry_kafka.dto.StoreOrderResponse;

@Service
public class WebSocketService {

    private final SimpMessagingTemplate messagingTemplate;

    public WebSocketService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    public void sendOrderUpdate(Integer orderId, ClientOrderResponse clientPayload, StoreOrderResponse storePayload) {
        messagingTemplate.convertAndSend("/topic/orders/" + orderId, clientPayload);
        messagingTemplate.convertAndSend("/topic/orders", storePayload);
    }

    public void sendLeadItemAlert(LeadItemEvent event) {
        messagingTemplate.convertAndSend("/topic/lead-items", event);
    }

    public void sendHotItemAlert(HotItemEvent event) {
        messagingTemplate.convertAndSend("/topic/hot-items", event);
    }

    public void sendAbandonedCartAlert(AbandonedCartEvent event) {
        messagingTemplate.convertAndSend("/topic/abandoned-carts", event);
    }
}

