package br.ufes.inf.soe.hungry_kafka.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Embeddable
public class ClientCategoryPreferenceId implements Serializable {

    @Column(name = "client_id")
    private Integer clientId;

    @Column(name = "category_id")
    private Integer categoryId;
}
