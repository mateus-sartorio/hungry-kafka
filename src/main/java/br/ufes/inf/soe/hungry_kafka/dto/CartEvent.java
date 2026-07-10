package br.ufes.inf.soe.hungry_kafka.dto;

public record CartEvent(
        CartAction action,
        Integer currentAmount,
        Integer productId,
        Integer clientId) {
}
