package br.ufes.inf.soe.queue_sine.dto;

import java.time.Instant;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class OrderEvent {

    private String eventId;
    private String clientId;
    private List<Integer> productIds;
    private Instant occurredAt;

    public OrderEvent() {
    }
}
