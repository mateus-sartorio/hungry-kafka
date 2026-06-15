package br.ufes.inf.soe.hungry_kafka.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import br.ufes.inf.soe.hungry_kafka.entity.OrderItem;

import java.util.List;

@Repository
public interface OrderItemRepository extends JpaRepository<OrderItem, Integer> {

	List<OrderItem> findByOrder_Id(Integer orderId);

}
