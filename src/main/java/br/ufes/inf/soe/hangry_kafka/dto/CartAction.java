package br.ufes.inf.soe.hangry_kafka.dto;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum CartAction {
    ADDED,
    REMOVED;

    @JsonCreator
    public static CartAction fromString(String value) {
        if (value == null) {
            return null;
        }
        try {
            return CartAction.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown CartAction: " + value);
        }
    }
}
