package br.ufes.inf.soe.queue_sine.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "product")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Product {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(nullable = false)
    private String name;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(nullable = false)
    private Double price;

    @Column(name = "photo_url", columnDefinition = "TEXT")
    private String photoUrl;

    @ManyToOne
    @JoinColumn(name = "category_id", nullable = false)
    private Category category;
}
