package br.ufes.inf.soe.hungry_kafka.dto;

import java.util.HashSet;
import java.util.Set;

public record AbandonedCartState(Set<Integer> productIds, boolean ordered) {
    public AbandonedCartState() {
        this(new HashSet<>(), false);
    }
}
