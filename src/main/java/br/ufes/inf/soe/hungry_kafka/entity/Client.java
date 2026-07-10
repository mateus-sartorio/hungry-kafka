package br.ufes.inf.soe.hungry_kafka.entity;

import java.util.HashMap;
import java.util.Map;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.MapKey;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Entity
@Table(name = "client")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Client {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(nullable = false)
    private String name;

    @OneToMany(mappedBy = "client", cascade = CascadeType.ALL, orphanRemoval = true)
    @MapKey(name = "id.productId")
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private Map<Integer, ClientProductPreference> productPreferences = new HashMap<>();

    @OneToMany(mappedBy = "client", cascade = CascadeType.ALL, orphanRemoval = true)
    @MapKey(name = "id.categoryId")
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private Map<Integer, ClientCategoryPreference> categoryPreferences = new HashMap<>();
}
