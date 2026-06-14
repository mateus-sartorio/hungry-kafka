package br.ufes.inf.soe.hangry_kafka.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

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
