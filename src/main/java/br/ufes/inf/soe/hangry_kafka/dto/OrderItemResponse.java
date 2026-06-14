package br.ufes.inf.soe.hangry_kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class OrderItemResponse {
    private ProductResponse product;
    private Integer amount;
}
