package br.ufes.inf.soe.queue_sine.repository;

import br.ufes.inf.soe.queue_sine.entity.Client;
import br.ufes.inf.soe.queue_sine.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, Integer> {

	List<OrderEntity> findByClient_Id(Integer clientId);

}
