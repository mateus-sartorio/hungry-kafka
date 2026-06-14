package br.ufes.inf.soe.hangry_kafka.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.ufes.inf.soe.hangry_kafka.dto.ClientDto;
import br.ufes.inf.soe.hangry_kafka.dto.OrderItemResponse;
import br.ufes.inf.soe.hangry_kafka.dto.OrderResponse;
import br.ufes.inf.soe.hangry_kafka.dto.ProductResponse;
import br.ufes.inf.soe.hangry_kafka.dto.StoreOrderResponse;
import br.ufes.inf.soe.hangry_kafka.entity.Client;
import br.ufes.inf.soe.hangry_kafka.entity.OrderEntity;
import br.ufes.inf.soe.hangry_kafka.entity.OrderItem;
import br.ufes.inf.soe.hangry_kafka.entity.Product;
import br.ufes.inf.soe.hangry_kafka.repository.OrderItemRepository;
import br.ufes.inf.soe.hangry_kafka.repository.OrderRepository;
import br.ufes.inf.soe.hangry_kafka.repository.ProductRepository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final ProductRepository productRepository;

    public OrderController(OrderRepository orderRepository,
                           OrderItemRepository orderItemRepository,
                           ProductRepository productRepository) {
        this.orderRepository = orderRepository;
        this.orderItemRepository = orderItemRepository;
        this.productRepository = productRepository;
    }

    @GetMapping
    public ResponseEntity<List<StoreOrderResponse>> listAllOrders() {
        List<StoreOrderResponse> orders = orderRepository.findAll().stream()
                .map(this::toStoreResponse)
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
        Integer clientId = order.getClient() != null ? order.getClient().getId() : null;
        String status = order.getStatus() != null ? order.getStatus().getName() : null;
        return new OrderResponse(order.getId(), clientId, buildItemResponses(order), order.getCreatedAt(), order.getExpectedDelivery(), status);
    }

    private StoreOrderResponse toStoreResponse(OrderEntity order) {
        Client client = order.getClient();
        ClientDto clientDto = client != null ? new ClientDto(client.getId(), client.getName()) : null;
        String status = order.getStatus() != null ? order.getStatus().getName() : null;
        return new StoreOrderResponse(order.getId(), clientDto, buildItemResponses(order), order.getCreatedAt(), order.getExpectedDelivery(), status);
    }

    private List<OrderItemResponse> buildItemResponses(OrderEntity order) {
        List<OrderItem> items = orderItemRepository.findByOrder_Id(order.getId());

        Set<Integer> productIds = new HashSet<>();
        for (OrderItem item : items) {
            productIds.add(item.getProductId());
        }

        Map<Integer, Product> productsById = new HashMap<>();
        for (Product product : productRepository.findAllById(productIds)) {
            productsById.put(product.getId(), product);
        }

        List<OrderItemResponse> itemResponses = new ArrayList<>();
        for (OrderItem item : items) {
            Product product = productsById.get(item.getProductId());
            if (product == null) {
                continue;
            }
            itemResponses.add(new OrderItemResponse(toProductResponse(product), item.getQuantity()));
        }
        return itemResponses;
    }

    private ProductResponse toProductResponse(Product product) {
        return new ProductResponse(
                product.getId(),
                product.getName(),
                product.getDescription(),
                product.getPrice(),
                product.getPhotoUrl(),
                1.0f
        );
    }
}
