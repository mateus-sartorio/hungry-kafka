package br.ufes.inf.soe.queue_sine.kafka;

import br.ufes.inf.soe.queue_sine.dto.CreateOrderRequest;
import br.ufes.inf.soe.queue_sine.dto.OrderItemInput;
import br.ufes.inf.soe.queue_sine.dto.OrderStatus;
import br.ufes.inf.soe.queue_sine.dto.OrderStatusEvent;
import br.ufes.inf.soe.queue_sine.entity.Client;
import br.ufes.inf.soe.queue_sine.entity.OrderEntity;
import br.ufes.inf.soe.queue_sine.entity.OrderItem;
import br.ufes.inf.soe.queue_sine.entity.OrderStatusEntity;
import br.ufes.inf.soe.queue_sine.repository.ClientRepository;
import br.ufes.inf.soe.queue_sine.repository.OrderItemRepository;
import br.ufes.inf.soe.queue_sine.repository.OrderRepository;
import br.ufes.inf.soe.queue_sine.repository.OrderStatusRepository;
import br.ufes.inf.soe.queue_sine.repository.ProductRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
public class KafkaListeners {

    private static final String INITIAL_STATUS_NAME = "CREATED";

    private final Logger logger = LoggerFactory.getLogger(KafkaListeners.class);

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final ClientRepository clientRepository;
    private final OrderStatusRepository orderStatusRepository;
    private final ProductRepository productRepository;

    public KafkaListeners(OrderRepository orderRepository,
                          OrderItemRepository orderItemRepository,
                          ClientRepository clientRepository,
                          OrderStatusRepository orderStatusRepository,
                          ProductRepository productRepository) {
        this.orderRepository = orderRepository;
        this.orderItemRepository = orderItemRepository;
        this.clientRepository = clientRepository;
        this.orderStatusRepository = orderStatusRepository;
        this.productRepository = productRepository;
    }

    @KafkaListener(topics = "item-view-events", groupId = "queue-sine-group")
    public void handleItemView(ConsumerRecord<String, String> record) {
        logger.info("Received event topic={} key={} payload={}", record.topic(), record.key(), record.value());
        // TODO: handle telemetry item view event
    }

    @KafkaListener(topics = "click-stream-events", groupId = "queue-sine-group")
    public void handleClickStream(ConsumerRecord<String, String> record) {
        logger.info("Received event topic={} key={} payload={}", record.topic(), record.key(), record.value());
        // TODO: handle click stream event
    }

    @KafkaListener(topics = "cart-events", groupId = "queue-sine-group")
    public void handleCartEvent(ConsumerRecord<String, String> record) {
        logger.info("Received event topic={} key={} payload={}", record.topic(), record.key(), record.value());
        // TODO: handle cart event
    }

    @KafkaListener(topics = "order-status-events", groupId = "queue-sine-group")
    @Transactional
    public void handleOrderStatusEvent(ConsumerRecord<String, String> record) {
        logger.info("Received event topic={} key={} payload={}", record.topic(), record.key(), record.value());

        OrderStatusEvent event;
        try {
            event = objectMapper.readValue(record.value(), OrderStatusEvent.class);
        } catch (JsonProcessingException e) {
            logger.warn("Invalid JSON for order-status topic, skipping: {}", e.getOriginalMessage());
            return;
        }

        if (event.getOrderId() == null || event.getOrderId().isBlank()) {
            logger.warn("Skipping order-status event: orderId is required");
            return;
        }
        if (event.getStatus() == null) {
            logger.warn("Skipping order-status event: status is required");
            return;
        }

        int orderId;
        try {
            orderId = Integer.parseInt(event.getOrderId().trim());
        } catch (NumberFormatException e) {
            logger.warn("Skipping order-status event: invalid orderId {}", event.getOrderId());
            return;
        }

        OrderEntity order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            logger.warn("Skipping order-status event: order not found id={}", orderId);
            return;
        }

        String current = order.getStatus() != null ? order.getStatus().getName() : null;
        if (current == null) {
            logger.warn("Skipping order-status event: order id={} has no status", orderId);
            return;
        }

        String target = toPersistedStatusName(event.getStatus());
        if (current.equals(target)) {
            logger.info("Order id={} already in status {}", orderId, target);
            return;
        }
        if (!isAllowedStatusTransition(current, target)) {
            logger.warn("Invalid status transition orderId={} from={} to={}", orderId, current, target);
            return;
        }

        if ("OUT_FOR_DELIVERY".equals(target) && event.getExpectedDelivery() == null) {
            logger.warn("Skipping order-status event: expectedDelivery is required when transitioning to OUT_FOR_DELIVERY orderId={}", orderId);
            return;
        }

        OrderStatusEntity next = orderStatusRepository.findByName(target).orElse(null);
        if (next == null) {
            logger.error("order_status '{}' is not configured", target);
            return;
        }

        order.setStatus(next);
        if ("OUT_FOR_DELIVERY".equals(target)) {
            order.setExpectedDelivery(Instant.now().plus(event.getExpectedDelivery()));
        }
        orderRepository.save(order);
        logger.info("Updated order id={} status {} -> {}", orderId, current, target);
    }

    @KafkaListener(topics = "order", groupId = "queue-sine-group")
    @Transactional
    public void handleOrderEvent(ConsumerRecord<String, String> record) {
        logger.info("Received event topic={} key={} payload={}", record.topic(), record.key(), record.value());

        CreateOrderRequest payload;
        try {
            payload = objectMapper.readValue(record.value(), CreateOrderRequest.class);
        } catch (JsonProcessingException e) {
            logger.warn("Invalid JSON for order topic, skipping: {}", e.getOriginalMessage());
            return;
        }

        if (payload.getClientId() == null) {
            logger.warn("Skipping order event: clientId is required");
            return;
        }
        if (payload.getItems() == null || payload.getItems().isEmpty()) {
            logger.warn("Skipping order event: items must be a non-empty array");
            return;
        }
        for (int i = 0; i < payload.getItems().size(); i++) {
            OrderItemInput line = payload.getItems().get(i);
            if (line == null || line.getProductId() == null
                    || line.getQuantity() == null || line.getQuantity() < 1) {
                logger.warn("Skipping order event: invalid item at index {}", i);
                return;
            }
        }

        Client client = clientRepository.findById(payload.getClientId()).orElse(null);
        if (client == null) {
            logger.warn("Skipping order event: client not found id={}", payload.getClientId());
            return;
        }

        Set<Integer> productIds = new HashSet<>();
        for (OrderItemInput line : payload.getItems()) {
            productIds.add(line.getProductId());
        }
        if (productRepository.findAllById(productIds).size() != productIds.size()) {
            logger.warn("Skipping order event: one or more products do not exist for clientId={}", payload.getClientId());
            return;
        }

        OrderStatusEntity status = orderStatusRepository.findByName(INITIAL_STATUS_NAME).orElse(null);
        if (status == null) {
            logger.error("Cannot persist order: order_status '{}' is not configured", INITIAL_STATUS_NAME);
            return;
        }

        OrderEntity order = new OrderEntity();
        order.setClient(client);
        order.setStatus(status);
        order.setCreatedAt(Instant.now());
        OrderEntity saved = orderRepository.save(order);

        List<OrderItem> rows = payload.getItems().stream()
                .map(line -> new OrderItem(null, saved, line.getProductId(), line.getQuantity()))
                .toList();
        orderItemRepository.saveAll(rows);

        logger.info("Persisted order id={} clientId={}", saved.getId(), payload.getClientId());
    }

    /** {@link OrderStatus} names match {@code order_status.name} values from Flyway (except {@code CREATED}). */
    private static String toPersistedStatusName(OrderStatus status) {
        return status.name();
    }

    private static boolean isAllowedStatusTransition(String current, String target) {
        return switch (current) {
            case "CREATED" -> "ACCEPTED".equals(target) || "CANCELLED".equals(target);
            case "ACCEPTED" -> "PREPARING".equals(target);
            case "PREPARING" -> "OUT_FOR_DELIVERY".equals(target);
            case "OUT_FOR_DELIVERY" -> "DELIVERED".equals(target);
            case "CANCELLED" -> false;
            default -> false;
        };
    }
}
