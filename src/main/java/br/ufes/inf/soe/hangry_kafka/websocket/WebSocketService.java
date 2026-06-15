package br.ufes.inf.soe.hangry_kafka.websocket;

import java.util.Map;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

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

    public void sendHotItemAlert(Integer productId) {
        messagingTemplate.convertAndSend("/topic/hot-items", (Object) Map.of("productId", productId));
    }
}
