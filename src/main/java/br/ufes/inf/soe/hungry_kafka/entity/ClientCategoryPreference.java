package br.ufes.inf.soe.hungry_kafka.entity;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnore;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MapsId;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

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
    @JsonIgnore
    private Client client;

    @Column(nullable = false)
    private Float value;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
