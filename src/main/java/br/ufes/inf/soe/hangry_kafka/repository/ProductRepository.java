package br.ufes.inf.soe.hangry_kafka.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import br.ufes.inf.soe.hangry_kafka.entity.Product;

@Repository
public interface ProductRepository extends JpaRepository<Product, Integer> {

}
