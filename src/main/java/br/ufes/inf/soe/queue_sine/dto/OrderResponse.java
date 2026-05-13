package br.ufes.inf.soe.queue_sine.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Instant;
import java.util.Map;

@Getter
@AllArgsConstructor
public class OrderResponse {
    private Integer id;
    private Integer clientId;
    private Map<Integer, Integer> items;
    private Instant createdAt;
    private String status;
}
