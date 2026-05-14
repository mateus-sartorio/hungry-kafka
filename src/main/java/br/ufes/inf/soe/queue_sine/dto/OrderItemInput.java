package br.ufes.inf.soe.queue_sine.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class OrderItemInput {

    private Integer productId;
    private Integer quantity;
}
