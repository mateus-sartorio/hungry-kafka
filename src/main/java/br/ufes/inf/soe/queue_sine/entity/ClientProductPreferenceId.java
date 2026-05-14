package br.ufes.inf.soe.queue_sine.entity;

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
public class ClientProductPreferenceId implements Serializable {

    @Column(name = "client_id")
    private Integer clientId;

    @Column(name = "product_id")
    private Integer productId;
}
