package br.ufes.inf.soe.queue_sine.dto;

import java.time.Instant;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class CartEvent {

    private String eventId;
    private String userId;
    private String cartId;
    private String itemId;
    private String category;
    private CartAction action;
    private int quantity;
    private Instant occurredAt;

    public CartEvent() {
    }

}
