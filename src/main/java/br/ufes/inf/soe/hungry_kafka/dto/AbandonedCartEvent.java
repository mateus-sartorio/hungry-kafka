package br.ufes.inf.soe.hungry_kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class AbandonedCartEvent {
    private Integer clientId;
    private List<Integer> productIds;
}
