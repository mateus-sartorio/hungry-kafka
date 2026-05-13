package br.ufes.inf.soe.queue_sine.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Instant;
import java.util.List;

@Getter
@AllArgsConstructor
public class OrderResponse {
    private Integer id;
    private Integer clientId;
    private List<OrderItemResponse> items;
    private Instant createdAt;
    private String status;
}
