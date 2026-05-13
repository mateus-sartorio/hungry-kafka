package br.ufes.inf.soe.queue_sine.dto;

import java.time.Instant;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ItemViewEvent {

    private String eventId;
    private String userId;
    private String itemId;
    private String category;
    private long viewDurationSeconds;
    private Instant observedAt;

    public ItemViewEvent() {
    }

}
