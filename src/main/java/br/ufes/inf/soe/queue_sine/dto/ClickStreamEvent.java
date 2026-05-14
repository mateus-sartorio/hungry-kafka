package br.ufes.inf.soe.queue_sine.dto;

import java.time.Instant;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ClickStreamEvent {

    private String eventId;
    private String userId;
    private String targetType;
    private String targetId;
    private String itemId;
    private String category;
    private Instant clickedAt;

    public ClickStreamEvent() {
    }

}
