package br.ufes.inf.soe.queue_sine.kafka;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import br.ufes.inf.soe.queue_sine.dto.CartEvent;
import br.ufes.inf.soe.queue_sine.dto.ClientDto;
import br.ufes.inf.soe.queue_sine.dto.CreateOrderRequest;
import br.ufes.inf.soe.queue_sine.dto.ItemViewEvent;
import br.ufes.inf.soe.queue_sine.dto.OrderItemInput;
import br.ufes.inf.soe.queue_sine.dto.OrderItemResponse;
import br.ufes.inf.soe.queue_sine.dto.OrderResponse;
import br.ufes.inf.soe.queue_sine.dto.OrderStatus;
import br.ufes.inf.soe.queue_sine.dto.OrderStatusEvent;
import br.ufes.inf.soe.queue_sine.dto.ProductResponse;
import br.ufes.inf.soe.queue_sine.dto.StoreOrderResponse;
import br.ufes.inf.soe.queue_sine.entity.Client;
import br.ufes.inf.soe.queue_sine.entity.ClientCategoryPreference;
import br.ufes.inf.soe.queue_sine.entity.ClientCategoryPreferenceId;
import br.ufes.inf.soe.queue_sine.entity.ClientProductPreference;
import br.ufes.inf.soe.queue_sine.entity.ClientProductPreferenceId;
import br.ufes.inf.soe.queue_sine.entity.OrderEntity;
import br.ufes.inf.soe.queue_sine.entity.OrderItem;
import br.ufes.inf.soe.queue_sine.entity.OrderStatusEntity;
import br.ufes.inf.soe.queue_sine.entity.Product;
import br.ufes.inf.soe.queue_sine.repository.ClientCategoryPreferenceRepository;
import br.ufes.inf.soe.queue_sine.repository.ClientProductPreferenceRepository;
import br.ufes.inf.soe.queue_sine.repository.ClientRepository;
import br.ufes.inf.soe.queue_sine.repository.OrderItemRepository;
import br.ufes.inf.soe.queue_sine.repository.OrderRepository;
import br.ufes.inf.soe.queue_sine.repository.OrderStatusRepository;
import br.ufes.inf.soe.queue_sine.repository.ProductRepository;

@Component
public class KafkaListeners {

    private static final String INITIAL_STATUS_NAME = "CREATED";

    private final Logger logger = LoggerFactory.getLogger(KafkaListeners.class);

    @Value("${app.preference.cart.multiplier:1.05}")
    private Float cartPreferenceMultiplier;

    @Value("${app.preference.itemview.multiplier:1.01}")
    private Float itemviewPreferenceMultiplier;

    @Value("${app.preference.order.multiplier:1.01}")
    private Float orderPreferenceMultiplier;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final ClientRepository clientRepository;
    private final OrderStatusRepository orderStatusRepository;
    private final ProductRepository productRepository;
    private final ClientCategoryPreferenceRepository clientCategoryPreferenceRepository;
    private final ClientProductPreferenceRepository clientProductPreferenceRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaListeners(OrderRepository orderRepository,
            OrderItemRepository orderItemRepository,
            ClientRepository clientRepository,
            OrderStatusRepository orderStatusRepository,
            ProductRepository productRepository,
            ClientCategoryPreferenceRepository clientCategoryPreferenceRepository,
            ClientProductPreferenceRepository clientProductPreferenceRepository,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.orderRepository = orderRepository;
        this.orderItemRepository = orderItemRepository;
        this.clientRepository = clientRepository;
        this.orderStatusRepository = orderStatusRepository;
        this.productRepository = productRepository;
        this.clientCategoryPreferenceRepository = clientCategoryPreferenceRepository;
        this.clientProductPreferenceRepository = clientProductPreferenceRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "item-view-events", groupId = "queue-sine-group")
    @Transactional
    public void handleItemView(ConsumerRecord<String, String> record) {
        logger.info("Received event topic={} key={} payload={}", record.topic(), record.key(), record.value());

        ItemViewEvent event;
        try {
            event = objectMapper.readValue(record.value(), ItemViewEvent.class);
        } catch (JsonProcessingException e) {
            logger.warn("Invalid JSON for item-view-events topic, skipping: {}", e.getOriginalMessage());
            return;
        }

        // Validate required fields
        if (event.getProductId() == null) {
            logger.warn("Skipping item-view event: productId is required");
            return;
        }
        if (event.getClientId() == null) {
            logger.warn("Skipping item-view event: clientId is required");
            return;
        }

        // Verify client exists
        Client client = clientRepository.findById(event.getClientId()).orElse(null);
        if (client == null) {
            logger.warn("Skipping item-view event: client not found id={}", event.getClientId());
            return;
        }

        // Verify product exists and get its category
        Product product = productRepository.findById(event.getProductId()).orElse(null);
        if (product == null) {
            logger.warn("Skipping item-view event: product not found id={}", event.getProductId());
            return;
        }

        Integer categoryId = product.getCategory() != null ? product.getCategory().getId() : null;
        if (categoryId == null) {
            logger.warn("Skipping item-view event: product id={} has no category", event.getProductId());
            return;
        }

        // Update or create client category preference
        ClientCategoryPreferenceId categoryPrefId = new ClientCategoryPreferenceId(event.getClientId(), categoryId);
        ClientCategoryPreference categoryPref = clientCategoryPreferenceRepository.findById(categoryPrefId)
                .orElse(null);

        if (categoryPref == null) {
            // Create new category preference with initial multiplier value
            categoryPref = new ClientCategoryPreference();
            categoryPref.setId(categoryPrefId);
            categoryPref.setClient(client);
            categoryPref.setValue(itemviewPreferenceMultiplier);
            categoryPref.setUpdatedAt(Instant.now());
            clientCategoryPreferenceRepository.save(categoryPref);
            logger.info("Created category preference clientId={} categoryId={} value={}", event.getClientId(),
                    categoryId, itemviewPreferenceMultiplier);
        } else {
            // Update existing category preference by preference multiplier
            Float newValue = categoryPref.getValue() * itemviewPreferenceMultiplier;
            categoryPref.setValue(newValue);
            categoryPref.setUpdatedAt(Instant.now());
            clientCategoryPreferenceRepository.save(categoryPref);
            logger.info("Updated category preference clientId={} categoryId={} newValue={}", event.getClientId(),
                    categoryId, newValue);
        }

        // Update or create client product preference
        ClientProductPreferenceId productPrefId = new ClientProductPreferenceId(event.getClientId(),
                event.getProductId());
        ClientProductPreference productPref = clientProductPreferenceRepository.findById(productPrefId)
                .orElse(null);

        if (productPref == null) {
            // Create new product preference with initial multiplier value
            productPref = new ClientProductPreference();
            productPref.setId(productPrefId);
            productPref.setClient(client);
            productPref.setValue(itemviewPreferenceMultiplier);
            productPref.setUpdatedAt(Instant.now());
            clientProductPreferenceRepository.save(productPref);
            logger.info("Created product preference clientId={} productId={} value={}", event.getClientId(),
                    event.getProductId(), itemviewPreferenceMultiplier);
        } else {
            // Update existing product preference by preference multiplier
            Float newValue = productPref.getValue() * itemviewPreferenceMultiplier;
            productPref.setValue(newValue);
            productPref.setUpdatedAt(Instant.now());
            clientProductPreferenceRepository.save(productPref);
            logger.info("Updated product preference clientId={} productId={} newValue={}", event.getClientId(),
                    event.getProductId(), newValue);
        }
    }

    @KafkaListener(topics = "cart-events", groupId = "queue-sine-group")
    @Transactional
    public void handleCartEvent(ConsumerRecord<String, String> record) {
        logger.info("Received event topic={} key={} payload={}", record.topic(), record.key(), record.value());

        CartEvent event;
        try {
            event = objectMapper.readValue(record.value(), CartEvent.class);
        } catch (JsonProcessingException e) {
            logger.warn("Invalid JSON for cart-events topic, skipping: {}", e.getOriginalMessage());
            return;
        }

        // Validate required fields
        if (event.getAction() == null) {
            logger.warn("Skipping cart event: action is required");
            return;
        }
        if (event.getProductId() == null) {
            logger.warn("Skipping cart event: productId is required");
            return;
        }
        if (event.getClientId() == null) {
            logger.warn("Skipping cart event: clientId is required");
            return;
        }

        // Only process ADDED events
        if (!event.getAction().name().equals("ADDED")) {
            logger.info("Skipping cart event: only ADDED events are processed, received {}", event.getAction());
            return;
        }

        // Verify client exists
        Client client = clientRepository.findById(event.getClientId()).orElse(null);
        if (client == null) {
            logger.warn("Skipping cart event: client not found id={}", event.getClientId());
            return;
        }

        // Verify product exists and get its category
        Product product = productRepository.findById(event.getProductId()).orElse(null);
        if (product == null) {
            logger.warn("Skipping cart event: product not found id={}", event.getProductId());
            return;
        }

        Integer categoryId = product.getCategory() != null ? product.getCategory().getId() : null;
        if (categoryId == null) {
            logger.warn("Skipping cart event: product id={} has no category", event.getProductId());
            return;
        }

        // Update or create client category preference
        ClientCategoryPreferenceId categoryPrefId = new ClientCategoryPreferenceId(event.getClientId(), categoryId);
        ClientCategoryPreference categoryPref = clientCategoryPreferenceRepository.findById(categoryPrefId)
                .orElse(null);

        if (categoryPref == null) {
            // Create new category preference with initial cart multiplier value
            categoryPref = new ClientCategoryPreference();
            categoryPref.setId(categoryPrefId);
            categoryPref.setClient(client);
            categoryPref.setValue(cartPreferenceMultiplier);
            categoryPref.setUpdatedAt(Instant.now());
            clientCategoryPreferenceRepository.save(categoryPref);
            logger.info("Created category preference clientId={} categoryId={} value={}", event.getClientId(),
                    categoryId, cartPreferenceMultiplier);
        } else {
            // Update existing category preference by cart multiplier
            Float newValue = categoryPref.getValue() * cartPreferenceMultiplier;
            categoryPref.setValue(newValue);
            categoryPref.setUpdatedAt(Instant.now());
            clientCategoryPreferenceRepository.save(categoryPref);
            logger.info("Updated category preference clientId={} categoryId={} newValue={}", event.getClientId(),
                    categoryId, newValue);
        }

        // Update or create client product preference
        ClientProductPreferenceId productPrefId = new ClientProductPreferenceId(event.getClientId(),
                event.getProductId());
        ClientProductPreference productPref = clientProductPreferenceRepository.findById(productPrefId)
                .orElse(null);

        if (productPref == null) {
            // Create new product preference with initial cart multiplier value
            productPref = new ClientProductPreference();
            productPref.setId(productPrefId);
            productPref.setClient(client);
            productPref.setValue(cartPreferenceMultiplier);
            productPref.setUpdatedAt(Instant.now());
            clientProductPreferenceRepository.save(productPref);
            logger.info("Created product preference clientId={} productId={} value={}", event.getClientId(),
                    event.getProductId(), cartPreferenceMultiplier);
        } else {
            // Update existing product preference by cart multiplier
            Float newValue = productPref.getValue() * cartPreferenceMultiplier;
            productPref.setValue(newValue);
            productPref.setUpdatedAt(Instant.now());
            clientProductPreferenceRepository.save(productPref);
            logger.info("Updated product preference clientId={} productId={} newValue={}", event.getClientId(),
                    event.getProductId(), newValue);
        }
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
            logger.warn(
                    "Skipping order-status event: expectedDelivery is required when transitioning to OUT_FOR_DELIVERY orderId={}",
                    orderId);
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

        StoreOrderResponse storeOrderResponse = toStoreResponse(order);
        OrderResponse orderResponse = toResponse(order);

        kafkaTemplate.send("order-status-changed", String.valueOf(orderId), storeOrderResponse);

        Integer clientId = order.getClient() != null ? order.getClient().getId() : null;
        if (clientId != null) {
            String clientTopic = "order-status-changed-" + clientId;
            kafkaTemplate.send(clientTopic, String.valueOf(orderId), orderResponse);
        }
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
            logger.warn("Skipping order event: one or more products do not exist for clientId={}",
                    payload.getClientId());
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

        // Update preferences for each product in the order
        for (OrderItemInput item : payload.getItems()) {
            Product product = productRepository.findById(item.getProductId()).orElse(null);
            if (product == null || product.getCategory() == null) {
                continue;
            }

            Integer categoryId = product.getCategory().getId();

            // Update or create client category preference
            ClientCategoryPreferenceId categoryPrefId = new ClientCategoryPreferenceId(payload.getClientId(),
                    categoryId);
            ClientCategoryPreference categoryPref = clientCategoryPreferenceRepository.findById(categoryPrefId)
                    .orElse(null);

            if (categoryPref == null) {
                // Create new category preference with initial order multiplier value
                categoryPref = new ClientCategoryPreference();
                categoryPref.setId(categoryPrefId);
                categoryPref.setClient(client);
                categoryPref.setValue(orderPreferenceMultiplier);
                categoryPref.setUpdatedAt(Instant.now());
                clientCategoryPreferenceRepository.save(categoryPref);
                logger.info("Created category preference clientId={} categoryId={} value={}",
                        payload.getClientId(), categoryId, orderPreferenceMultiplier);
            } else {
                // Update existing category preference by order multiplier
                Float newValue = categoryPref.getValue() * orderPreferenceMultiplier;
                categoryPref.setValue(newValue);
                categoryPref.setUpdatedAt(Instant.now());
                clientCategoryPreferenceRepository.save(categoryPref);
                logger.info("Updated category preference clientId={} categoryId={} newValue={}",
                        payload.getClientId(), categoryId, newValue);
            }

            // Update or create client product preference
            ClientProductPreferenceId productPrefId = new ClientProductPreferenceId(payload.getClientId(),
                    item.getProductId());
            ClientProductPreference productPref = clientProductPreferenceRepository.findById(productPrefId)
                    .orElse(null);

            if (productPref == null) {
                // Create new product preference with initial order multiplier value
                productPref = new ClientProductPreference();
                productPref.setId(productPrefId);
                productPref.setClient(client);
                productPref.setValue(orderPreferenceMultiplier);
                productPref.setUpdatedAt(Instant.now());
                clientProductPreferenceRepository.save(productPref);
                logger.info("Created product preference clientId={} productId={} value={}",
                        payload.getClientId(), item.getProductId(), orderPreferenceMultiplier);
            } else {
                // Update existing product preference by order multiplier
                Float newValue = productPref.getValue() * orderPreferenceMultiplier;
                productPref.setValue(newValue);
                productPref.setUpdatedAt(Instant.now());
                clientProductPreferenceRepository.save(productPref);
                logger.info("Updated product preference clientId={} productId={} newValue={}",
                        payload.getClientId(), item.getProductId(), newValue);
            }
        }

        logger.info("Persisted order id={} clientId={}", saved.getId(), payload.getClientId());
    }

    /**
     * {@link OrderStatus} names match {@code order_status.name} values from Flyway
     * (except {@code CREATED}).
     */
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

    private OrderResponse toResponse(OrderEntity order) {
        Integer clientId = order.getClient() != null ? order.getClient().getId() : null;
        String status = order.getStatus() != null ? order.getStatus().getName() : null;
        return new OrderResponse(order.getId(), clientId, buildItemResponses(order), order.getCreatedAt(),
                order.getExpectedDelivery(), status);
    }

    private StoreOrderResponse toStoreResponse(OrderEntity order) {
        Client client = order.getClient();
        ClientDto clientDto = client != null ? new ClientDto(client.getId(), client.getName()) : null;
        String status = order.getStatus() != null ? order.getStatus().getName() : null;
        return new StoreOrderResponse(order.getId(), clientDto, buildItemResponses(order), order.getCreatedAt(),
                order.getExpectedDelivery(), status);
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
                1.0f);
    }
}
