package br.ufes.inf.soe.hungry_kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class LeadItemEvent {
    private Integer clientId;
    private Integer productId;
}
