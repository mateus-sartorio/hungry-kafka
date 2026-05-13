package br.ufes.inf.soe.queue_sine.repository;

import br.ufes.inf.soe.queue_sine.entity.OrderItem;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderItemRepository extends JpaRepository<OrderItem, Integer> {

}
