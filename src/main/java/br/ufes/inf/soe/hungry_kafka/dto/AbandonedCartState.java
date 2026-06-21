package br.ufes.inf.soe.hungry_kafka.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

/**
 * Per-client, per-window aggregate used by {@code AbandonedCartTopology}.
 * Tracks which products are still sitting in the cart and whether the client
 * placed an order during the window. A window that closes with items still in
 * the cart and {@code ordered == false} is an abandoned cart.
 */
@Setter
@Getter
@NoArgsConstructor
public class AbandonedCartState {

    private Set<Integer> productIds = new HashSet<>();
    private boolean ordered = false;
}
