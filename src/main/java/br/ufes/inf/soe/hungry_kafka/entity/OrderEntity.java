package br.ufes.inf.soe.hungry_kafka.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "orders")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @ManyToOne
    @JoinColumn(name = "client_id", nullable = false)
    private Client client;

    @ManyToOne
    @JoinColumn(name = "status_id", nullable = false)
    private OrderStatusEntity status;

    @Column(name = "created_at", nullable = false, columnDefinition = "TEXT")
    @Convert(converter = InstantTextAttributeConverter.class)
    private Instant createdAt;

    @Column(name = "expected_delivery", columnDefinition = "TEXT")
    @Convert(converter = InstantTextAttributeConverter.class)
    private Instant expectedDelivery;
}
