package br.ufes.inf.soe.hangry_kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Instant;
import java.util.List;

@Getter
@AllArgsConstructor
public class ClientOrderResponse {
    private Integer id;
    private Integer clientId;
    private List<OrderItemResponse> items;
    private Instant createdAt;
    private Instant expectedDelivery;
    private String status;
}
