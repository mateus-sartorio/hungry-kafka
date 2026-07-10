package br.ufes.inf.soe.hungry_kafka.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import br.ufes.inf.soe.hungry_kafka.entity.OrderEntity;

@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, Integer> {

	List<OrderEntity> findByClient_Id(Integer clientId);

}
