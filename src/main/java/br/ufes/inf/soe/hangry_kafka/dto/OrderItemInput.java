package br.ufes.inf.soe.hangry_kafka.dto;

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
