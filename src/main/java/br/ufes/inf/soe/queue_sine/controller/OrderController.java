package br.ufes.inf.soe.queue_sine.controller;

import br.ufes.inf.soe.queue_sine.dto.OrderResponse;
import br.ufes.inf.soe.queue_sine.entity.OrderEntity;
import br.ufes.inf.soe.queue_sine.entity.OrderItem;
import br.ufes.inf.soe.queue_sine.repository.OrderItemRepository;
import br.ufes.inf.soe.queue_sine.repository.OrderRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;

    public OrderController(OrderRepository orderRepository, OrderItemRepository orderItemRepository) {
        this.orderRepository = orderRepository;
        this.orderItemRepository = orderItemRepository;
    }

    @GetMapping
    public ResponseEntity<List<OrderResponse>> listAllOrders() {
        List<OrderResponse> orders = orderRepository.findAll().stream()
                .map(this::toResponse)
                .collect(Collectors.toList());
        return ResponseEntity.ok(orders);
    }

    @GetMapping("/client/{clientId}")
    public ResponseEntity<List<OrderResponse>> listOrdersByClient(@PathVariable Integer clientId) {
        List<OrderResponse> orders = orderRepository.findByClient_Id(clientId).stream()
                .map(this::toResponse)
                .collect(Collectors.toList());
        return ResponseEntity.ok(orders);
    }

    private OrderResponse toResponse(OrderEntity order) {
        List<OrderItem> items = orderItemRepository.findByOrder_Id(order.getId());
        Map<Integer, Integer> itemMap = new LinkedHashMap<>();
        for (OrderItem item : items) {
            itemMap.put(item.getProductId(), item.getQuantity());
        }

        Integer clientId = order.getClient() != null ? order.getClient().getId() : null;
        String status = order.getStatus() != null ? order.getStatus().getName() : null;

        return new OrderResponse(order.getId(), clientId, itemMap, order.getCreatedAt(), status);
    }
}
