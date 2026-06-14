package br.ufes.inf.soe.hangry_kafka.entity;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.time.Instant;

/**
 * SQLite stores {@code orders.created_at} as TEXT. Hibernate may persist {@link Instant}
 * as epoch millis, while the SQLite JDBC driver expects a formatted timestamp when reading.
 * This converter reads both millis strings and ISO-8601, and always writes ISO-8601.
 */
@Converter
public class InstantTextAttributeConverter implements AttributeConverter<Instant, String> {

    @Override
    public String convertToDatabaseColumn(Instant instant) {
        return instant == null ? null : instant.toString();
    }

    @Override
    public Instant convertToEntityAttribute(String dbData) {
        if (dbData == null || dbData.isBlank()) {
            return null;
        }
        String s = dbData.trim();
        if (s.chars().allMatch(Character::isDigit)) {
            return Instant.ofEpochMilli(Long.parseLong(s));
        }
        return Instant.parse(s);
    }
}
