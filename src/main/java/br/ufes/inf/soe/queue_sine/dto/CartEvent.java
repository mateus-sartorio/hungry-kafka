package br.ufes.inf.soe.queue_sine.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CartEvent {

    private String eventId;
    private CartAction action;
    private Integer currentAmount;
    private Integer productId;
    private Integer clientId;
    private Instant occurredAt;

}
