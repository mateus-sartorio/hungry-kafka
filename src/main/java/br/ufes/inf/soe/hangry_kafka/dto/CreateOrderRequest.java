package br.ufes.inf.soe.hangry_kafka.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class CreateOrderRequest {

    private Integer clientId;
    private List<OrderItemInput> items;
}
