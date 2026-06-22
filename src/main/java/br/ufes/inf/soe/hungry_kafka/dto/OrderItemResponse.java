package br.ufes.inf.soe.hungry_kafka.dto;

public record OrderItemResponse(ProductResponse product, Integer amount) {
}
