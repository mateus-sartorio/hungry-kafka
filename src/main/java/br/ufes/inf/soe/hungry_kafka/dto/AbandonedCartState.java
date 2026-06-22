package br.ufes.inf.soe.hungry_kafka.dto;

import java.util.HashSet;
import java.util.Set;

/**
 * Per-client, per-window aggregate used by {@code AbandonedCartTopology}.
 * Tracks which products are still sitting in the cart and whether the client
 * placed an order during the window. A window that closes with items still in
 * the cart and {@code ordered == false} is an abandoned cart.
 */
public record AbandonedCartState(Set<Integer> productIds, boolean ordered) {

    public AbandonedCartState() {
        this(new HashSet<>(), false);
    }
}
