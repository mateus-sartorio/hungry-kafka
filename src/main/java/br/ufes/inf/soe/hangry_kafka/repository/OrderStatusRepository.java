package br.ufes.inf.soe.hangry_kafka.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import br.ufes.inf.soe.hangry_kafka.entity.OrderStatusEntity;

import java.util.Optional;

@Repository
public interface OrderStatusRepository extends JpaRepository<OrderStatusEntity, Integer> {

    Optional<OrderStatusEntity> findByName(String name);
}
