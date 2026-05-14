package br.ufes.inf.soe.queue_sine.dto;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ItemViewEvent {

    private Integer productId;
    private Integer clientId;

    public ItemViewEvent() {
    }

}
