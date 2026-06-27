package br.ufes.inf.soe.hungry_kafka.entity;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.time.Instant;

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
