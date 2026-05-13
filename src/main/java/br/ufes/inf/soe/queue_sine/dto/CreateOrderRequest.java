package br.ufes.inf.soe.queue_sine.dto;

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
