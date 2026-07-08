package br.ufes.inf.soe.hungry_kafka.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import br.ufes.inf.soe.hungry_kafka.entity.OrderStatusEntity;

@Repository
public interface OrderStatusRepository extends JpaRepository<OrderStatusEntity, Integer> {

    Optional<OrderStatusEntity> findByName(String name);
}
