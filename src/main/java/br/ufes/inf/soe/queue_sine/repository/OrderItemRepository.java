package br.ufes.inf.soe.queue_sine.repository;

import br.ufes.inf.soe.queue_sine.entity.OrderItem;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderItemRepository extends JpaRepository<OrderItem, Integer> {

	List<OrderItem> findByOrder_Id(Integer orderId);

}
