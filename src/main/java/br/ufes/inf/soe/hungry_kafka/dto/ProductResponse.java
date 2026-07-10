package br.ufes.inf.soe.hungry_kafka.dto;

public record ProductResponse(
        Integer id,
        String name,
        String description,
        Double price,
        String photo,
        Float priority) {
}
