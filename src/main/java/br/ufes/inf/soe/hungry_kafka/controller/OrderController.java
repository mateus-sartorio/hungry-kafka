package br.ufes.inf.soe.hungry_kafka.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.ufes.inf.soe.hungry_kafka.dto.ClientDto;
import br.ufes.inf.soe.hungry_kafka.dto.ClientOrderResponse;
import br.ufes.inf.soe.hungry_kafka.dto.CreateOrderRequest;
import br.ufes.inf.soe.hungry_kafka.dto.OrderItemInput;
import br.ufes.inf.soe.hungry_kafka.dto.OrderItemResponse;
import br.ufes.inf.soe.hungry_kafka.dto.ProductResponse;
import br.ufes.inf.soe.hungry_kafka.dto.StoreOrderResponse;
import br.ufes.inf.soe.hungry_kafka.entity.Client;
import br.ufes.inf.soe.hungry_kafka.entity.ClientCategoryPreference;
import br.ufes.inf.soe.hungry_kafka.entity.ClientCategoryPreferenceId;
import br.ufes.inf.soe.hungry_kafka.entity.ClientProductPreference;
import br.ufes.inf.soe.hungry_kafka.entity.ClientProductPreferenceId;
import br.ufes.inf.soe.hungry_kafka.entity.OrderEntity;
import br.ufes.inf.soe.hungry_kafka.entity.OrderItem;
import br.ufes.inf.soe.hungry_kafka.entity.OrderStatusEntity;
import br.ufes.inf.soe.hungry_kafka.entity.Product;

import br.ufes.inf.soe.hungry_kafka.repository.ClientCategoryPreferenceRepository;
import br.ufes.inf.soe.hungry_kafka.repository.ClientProductPreferenceRepository;
import br.ufes.inf.soe.hungry_kafka.repository.ClientRepository;
import br.ufes.inf.soe.hungry_kafka.repository.OrderItemRepository;
import br.ufes.inf.soe.hungry_kafka.repository.OrderRepository;
import br.ufes.inf.soe.hungry_kafka.repository.OrderStatusRepository;
import br.ufes.inf.soe.hungry_kafka.repository.ProductRepository;
import br.ufes.inf.soe.hungry_kafka.producer.EventProducer;
import br.ufes.inf.soe.hungry_kafka.websocket.WebSocketService;

import java.time.Instant;
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

    private static final String INITIAL_STATUS_NAME = "CREATED";

    @Value("${app.preference.cart.multiplier:1.05}")
    private Float cartPreferenceMultiplier;

    @Value("${app.preference.itemview.multiplier:1.01}")
    private Float itemviewPreferenceMultiplier;

    @Value("${app.preference.order.multiplier:1.01}")
    private Float orderPreferenceMultiplier;

    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final ProductRepository productRepository;
    public final ClientRepository clientRepository;
    public final OrderStatusRepository orderStatusRepository;
    public final ClientCategoryPreferenceRepository clientCategoryPreferenceRepository;
    public final ClientProductPreferenceRepository clientProductPreferenceRepository;

    public final WebSocketService webSocketService;
    public final EventProducer eventProducer;

    public OrderController(
            OrderRepository orderRepository,
            OrderItemRepository orderItemRepository,
            ProductRepository productRepository,
            ClientRepository clientRepository,
            OrderStatusRepository orderStatusRepository,
            ClientCategoryPreferenceRepository clientCategoryPreferenceRepository,
            ClientProductPreferenceRepository clientProductPreferenceRepository,
            WebSocketService webSocketService,
            EventProducer eventProducer) {
        this.orderRepository = orderRepository;
        this.orderItemRepository = orderItemRepository;
        this.productRepository = productRepository;
        this.clientRepository = clientRepository;
        this.orderStatusRepository = orderStatusRepository;
        this.clientCategoryPreferenceRepository = clientCategoryPreferenceRepository;
        this.clientProductPreferenceRepository = clientProductPreferenceRepository;
        this.webSocketService = webSocketService;
        this.eventProducer = eventProducer;
    }

    @GetMapping
    public ResponseEntity<List<StoreOrderResponse>> listAllOrders() {
        List<StoreOrderResponse> orders = orderRepository.findAll().stream().map(this::toStoreResponse).collect(Collectors.toList());
        return ResponseEntity.ok(orders);
    }

    @GetMapping("/client/{clientId}")
    public ResponseEntity<List<ClientOrderResponse>> listOrdersByClient(@PathVariable Integer clientId) {
        List<ClientOrderResponse> orders = orderRepository.findByClient_Id(clientId).stream().map(this::toClientResponse).collect(Collectors.toList());
        return ResponseEntity.ok(orders);
    }

    @PostMapping
    public ResponseEntity<Void> createOrder(@RequestBody CreateOrderRequest request) {
        for (int i = 0; i < request.items().size(); i++) {
            OrderItemInput line = request.items().get(i);
            if (line == null || line.productId() == null || line.quantity() == null || line.quantity() < 1) {
                return ResponseEntity.badRequest().build();
            }
        }

        Client client = clientRepository.findById(request.clientId()).orElse(null);
        if (client == null) {
            return ResponseEntity.badRequest().build();
        }

        Set<Integer> productIds = new HashSet<>();
        for (OrderItemInput line : request.items()) {
            productIds.add(line.productId());
        }
        if (productRepository.findAllById(productIds).size() != productIds.size()) {
            return ResponseEntity.badRequest().build();
        }

        OrderStatusEntity status = orderStatusRepository.findByName(INITIAL_STATUS_NAME).orElse(null);
        if (status == null) {
            return ResponseEntity.badRequest().build();
        }

        OrderEntity order = new OrderEntity();
        order.setClient(client);
        order.setStatus(status);
        order.setCreatedAt(Instant.now());
        OrderEntity saved = orderRepository.save(order);

        List<OrderItem> rows = request.items().stream().map(line -> new OrderItem(null, saved, line.productId(), line.quantity())).toList();
        orderItemRepository.saveAll(rows);

        for (OrderItemInput item : request.items()) {
            Product product = productRepository.findById(item.productId()).orElse(null);
            if (product == null || product.getCategory() == null) {
                continue;
            }

            Integer categoryId = product.getCategory().getId();

            ClientCategoryPreferenceId categoryPrefId = new ClientCategoryPreferenceId(request.clientId(), categoryId);
            ClientCategoryPreference categoryPreference = clientCategoryPreferenceRepository.findById(categoryPrefId).orElse(null);

            if (categoryPreference == null) {
                categoryPreference = new ClientCategoryPreference();
                categoryPreference.setId(categoryPrefId);
                categoryPreference.setClient(client);
                categoryPreference.setValue(orderPreferenceMultiplier);
                categoryPreference.setUpdatedAt(Instant.now());
                clientCategoryPreferenceRepository.save(categoryPreference);
            } else {
                Float newValue = categoryPreference.getValue() * orderPreferenceMultiplier;
                categoryPreference.setValue(newValue);
                categoryPreference.setUpdatedAt(Instant.now());
                clientCategoryPreferenceRepository.save(categoryPreference);
            }

            ClientProductPreferenceId productPreferenceId = new ClientProductPreferenceId(request.clientId(), item.productId());
            ClientProductPreference productPreference = clientProductPreferenceRepository.findById(productPreferenceId).orElse(null);

            if (productPreference == null) {
                productPreference = new ClientProductPreference();
                productPreference.setId(productPreferenceId);
                productPreference.setClient(client);
                productPreference.setValue(orderPreferenceMultiplier);
                productPreference.setUpdatedAt(Instant.now());
                clientProductPreferenceRepository.save(productPreference);
            } else {
                Float newValue = productPreference.getValue() * orderPreferenceMultiplier;
                productPreference.setValue(newValue);
                productPreference.setUpdatedAt(Instant.now());
                clientProductPreferenceRepository.save(productPreference);
            }
        }

        webSocketService.sendOrderUpdate(saved.getId(), toClientResponse(saved), toStoreResponse(saved));

        eventProducer.sendOrder(request);

        return ResponseEntity.ok().build();
    }

    private ClientOrderResponse toClientResponse(OrderEntity order) {
        Integer clientId = order.getClient() != null ? order.getClient().getId() : null;
        String status = order.getStatus() != null ? order.getStatus().getName() : null;
        return new ClientOrderResponse(order.getId(), clientId, buildItemResponses(order), order.getCreatedAt(), order.getExpectedDelivery(), status);
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
        return new ProductResponse(product.getId(), product.getName(), product.getDescription(), product.getPrice(), product.getPhotoUrl(), 1.0f);
    }
}
