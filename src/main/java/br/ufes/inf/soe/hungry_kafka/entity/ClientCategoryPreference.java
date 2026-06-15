package br.ufes.inf.soe.hungry_kafka.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.EqualsAndHashCode;

import java.time.Instant;

@Entity
@Table(name = "client_category_preference")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientCategoryPreference {

    @EmbeddedId
    private ClientCategoryPreferenceId id = new ClientCategoryPreferenceId();

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
