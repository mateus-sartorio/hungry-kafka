package br.ufes.inf.soe.hangry_kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ClientDto {
    private Integer clientId;
    private String clientName;
}
