package br.ufes.inf.soe.hangry_kafka.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.EqualsAndHashCode;

import java.time.Instant;

@Entity
@Table(name = "client_product_preference")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientProductPreference {

    @EmbeddedId
    private ClientProductPreferenceId id = new ClientProductPreferenceId();

    @ManyToOne(fetch = FetchType.LAZY)
    @MapsId("clientId")
    @JoinColumn(name = "client_id")
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private Client client;

    @Column(nullable = false)
    private Float value;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
