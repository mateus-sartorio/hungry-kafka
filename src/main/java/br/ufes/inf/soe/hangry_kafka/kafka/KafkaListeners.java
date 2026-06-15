package br.ufes.inf.soe.hangry_kafka.kafka;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import br.ufes.inf.soe.hangry_kafka.config.TopicNames;
import br.ufes.inf.soe.hangry_kafka.dto.CartEvent;
import br.ufes.inf.soe.hangry_kafka.dto.ClientDto;
import br.ufes.inf.soe.hangry_kafka.dto.ItemViewEvent;
import br.ufes.inf.soe.hangry_kafka.dto.OrderItemResponse;
import br.ufes.inf.soe.hangry_kafka.dto.ClientOrderResponse;
import br.ufes.inf.soe.hangry_kafka.dto.OrderStatusEvent;
import br.ufes.inf.soe.hangry_kafka.dto.ProductResponse;
import br.ufes.inf.soe.hangry_kafka.dto.StoreOrderResponse;
import br.ufes.inf.soe.hangry_kafka.entity.Client;
import br.ufes.inf.soe.hangry_kafka.entity.ClientCategoryPreference;
import br.ufes.inf.soe.hangry_kafka.entity.ClientCategoryPreferenceId;
import br.ufes.inf.soe.hangry_kafka.entity.ClientProductPreference;
import br.ufes.inf.soe.hangry_kafka.entity.ClientProductPreferenceId;
import br.ufes.inf.soe.hangry_kafka.entity.OrderEntity;
import br.ufes.inf.soe.hangry_kafka.entity.OrderItem;
import br.ufes.inf.soe.hangry_kafka.entity.OrderStatusEntity;
import br.ufes.inf.soe.hangry_kafka.entity.Product;
import br.ufes.inf.soe.hangry_kafka.repository.ClientCategoryPreferenceRepository;
import br.ufes.inf.soe.hangry_kafka.repository.ClientProductPreferenceRepository;
import br.ufes.inf.soe.hangry_kafka.repository.ClientRepository;
import br.ufes.inf.soe.hangry_kafka.repository.OrderItemRepository;
import br.ufes.inf.soe.hangry_kafka.repository.OrderRepository;
import br.ufes.inf.soe.hangry_kafka.repository.OrderStatusRepository;
import br.ufes.inf.soe.hangry_kafka.repository.ProductRepository;
import br.ufes.inf.soe.hangry_kafka.websocket.WebSocketService;

@Component
public class KafkaListeners {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final ClientRepository clientRepository;
    private final OrderStatusRepository orderStatusRepository;
    private final ProductRepository productRepository;
    private final ClientCategoryPreferenceRepository clientCategoryPreferenceRepository;
    private final ClientProductPreferenceRepository clientProductPreferenceRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final WebSocketService webSocketService;

    @Value("${app.hot-item.threshold:10}")
    private int hotItemThreshold;

    @Value("${app.hot-item.duration-seconds:15}")
    private int hotItemDurationSeconds;

    @Value("${app.preference.cart.multiplier:1.05}")
    private Float cartPreferenceMultiplier;

    @Value("${app.preference.itemview.multiplier:1.01}")
    private Float itemviewPreferenceMultiplier;

    @Value("${app.preference.order.multiplier:1.01}")
    private Float orderPreferenceMultiplier;

    private final Map<Integer, List<Instant>> productBumps = new ConcurrentHashMap<>();

    public KafkaListeners(OrderRepository orderRepository, OrderItemRepository orderItemRepository, ClientRepository clientRepository, OrderStatusRepository orderStatusRepository, ProductRepository productRepository, ClientCategoryPreferenceRepository clientCategoryPreferenceRepository,
            ClientProductPreferenceRepository clientProductPreferenceRepository, KafkaTemplate<String, Object> kafkaTemplate, WebSocketService webSocketService) {
        this.orderRepository = orderRepository;
        this.orderItemRepository = orderItemRepository;
        this.clientRepository = clientRepository;
        this.orderStatusRepository = orderStatusRepository;
        this.productRepository = productRepository;
        this.clientCategoryPreferenceRepository = clientCategoryPreferenceRepository;
        this.clientProductPreferenceRepository = clientProductPreferenceRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.webSocketService = webSocketService;
    }

    public void recordHotItemHit(Integer productId) {
        productBumps.compute(productId, (id, timestamps) -> {
            if (timestamps == null) {
                timestamps = new ArrayList<>();
            }
            Instant now = Instant.now();
            timestamps.add(now);

            Instant cutoff = now.minusSeconds(hotItemDurationSeconds);
            timestamps.removeIf(t -> t.isBefore(cutoff));

            if (timestamps.size() >= hotItemThreshold) {
                kafkaTemplate.send(TopicNames.HOT_ITEM_EVENTS, String.valueOf(productId), "{\"productId\": " + productId + "}");
                webSocketService.sendHotItemAlert(productId);
                timestamps.clear();
            }
            return timestamps;
        });
    }

    @KafkaListener(topics = TopicNames.ITEM_VIEW_EVENTS, groupId = "hungry-kafka-group")
    @Transactional
    public void handleItemView(ConsumerRecord<String, String> record) {
        ItemViewEvent event;
        try {
            event = objectMapper.readValue(record.value(), ItemViewEvent.class);
        } catch (JsonProcessingException e) {
            return;
        }

        Client client = clientRepository.findById(event.getClientId()).orElse(null);
        if (client == null) {
            return;
        }

        Product product = productRepository.findById(event.getProductId()).orElse(null);
        if (product == null) {
            return;
        }

        Integer categoryId = product.getCategory() != null ? product.getCategory().getId() : null;
        if (categoryId == null) {
            return;
        }

        ClientCategoryPreferenceId categoryPrefId = new ClientCategoryPreferenceId(event.getClientId(), categoryId);
        ClientCategoryPreference categoryPref = clientCategoryPreferenceRepository.findById(categoryPrefId).orElse(null);

        if (categoryPref == null) {
            categoryPref = new ClientCategoryPreference();
            categoryPref.setId(categoryPrefId);
            categoryPref.setClient(client);
            categoryPref.setValue(itemviewPreferenceMultiplier);
            categoryPref.setUpdatedAt(Instant.now());
            clientCategoryPreferenceRepository.save(categoryPref);
        } else {
            Float newValue = categoryPref.getValue() * itemviewPreferenceMultiplier;
            categoryPref.setValue(newValue);
            categoryPref.setUpdatedAt(Instant.now());
            clientCategoryPreferenceRepository.save(categoryPref);
        }

        ClientProductPreferenceId productPrefId = new ClientProductPreferenceId(event.getClientId(), event.getProductId());
        ClientProductPreference productPref = clientProductPreferenceRepository.findById(productPrefId).orElse(null);

        if (productPref == null) {
            productPref = new ClientProductPreference();
            productPref.setId(productPrefId);
            productPref.setClient(client);
            productPref.setValue(itemviewPreferenceMultiplier);
            productPref.setUpdatedAt(Instant.now());
            clientProductPreferenceRepository.save(productPref);
        } else {
            Float newValue = productPref.getValue() * itemviewPreferenceMultiplier;
            productPref.setValue(newValue);
            productPref.setUpdatedAt(Instant.now());
            clientProductPreferenceRepository.save(productPref);
        }

        recordHotItemHit(event.getProductId());
    }

    @KafkaListener(topics = TopicNames.CART_EVENTS, groupId = "hungry-kafka-group")
    @Transactional
    public void handleCartEvent(ConsumerRecord<String, String> record) {
        CartEvent event;
        try {
            event = objectMapper.readValue(record.value(), CartEvent.class);
        } catch (JsonProcessingException e) {
            return;
        }

        if (!event.getAction().name().equals("ADDED")) {
            return;
        }

        Client client = clientRepository.findById(event.getClientId()).orElse(null);
        if (client == null) {
            return;
        }

        Product product = productRepository.findById(event.getProductId()).orElse(null);
        if (product == null) {
            return;
        }

        Integer categoryId = product.getCategory() != null ? product.getCategory().getId() : null;
        if (categoryId == null) {
            return;
        }

        ClientCategoryPreferenceId categoryPrefId = new ClientCategoryPreferenceId(event.getClientId(), categoryId);
        ClientCategoryPreference categoryPref = clientCategoryPreferenceRepository.findById(categoryPrefId).orElse(null);

        if (categoryPref == null) {
            categoryPref = new ClientCategoryPreference();
            categoryPref.setId(categoryPrefId);
            categoryPref.setClient(client);
            categoryPref.setValue(cartPreferenceMultiplier);
            categoryPref.setUpdatedAt(Instant.now());
            clientCategoryPreferenceRepository.save(categoryPref);
        } else {
            Float newValue = categoryPref.getValue() * cartPreferenceMultiplier;
            categoryPref.setValue(newValue);
            categoryPref.setUpdatedAt(Instant.now());
            clientCategoryPreferenceRepository.save(categoryPref);
        }

        ClientProductPreferenceId productPrefId = new ClientProductPreferenceId(event.getClientId(), event.getProductId());
        ClientProductPreference productPref = clientProductPreferenceRepository.findById(productPrefId).orElse(null);

        if (productPref == null) {
            productPref = new ClientProductPreference();
            productPref.setId(productPrefId);
            productPref.setClient(client);
            productPref.setValue(cartPreferenceMultiplier);
            productPref.setUpdatedAt(Instant.now());
            clientProductPreferenceRepository.save(productPref);
        } else {
            Float newValue = productPref.getValue() * cartPreferenceMultiplier;
            productPref.setValue(newValue);
            productPref.setUpdatedAt(Instant.now());
            clientProductPreferenceRepository.save(productPref);
        }

        recordHotItemHit(event.getProductId());
    }

    @KafkaListener(topics = TopicNames.ORDER_STATUS_EVENTS, groupId = "hungry-kafka-group")
    @Transactional
    public void handleOrderStatusEvent(ConsumerRecord<String, String> record) {
        OrderStatusEvent event;
        try {
            event = objectMapper.readValue(record.value(), OrderStatusEvent.class);
        } catch (JsonProcessingException e) {
            return;
        }

        int orderId;
        try {
            orderId = Integer.parseInt(event.getOrderId().trim());
        } catch (NumberFormatException e) {
            return;
        }

        OrderEntity order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }

        String current = order.getStatus() != null ? order.getStatus().getName() : null;
        if (current == null) {
            return;
        }

        String target = event.getStatus().name();
        if (current.equals(target)) {
            return;
        }
        if (!isAllowedStatusTransition(current, target)) {
            return;
        }

        if ("OUT_FOR_DELIVERY".equals(target) && event.getExpectedDelivery() == null) {
            return;
        }

        OrderStatusEntity next = orderStatusRepository.findByName(target).orElse(null);
        if (next == null) {
            return;
        }

        order.setStatus(next);
        if ("OUT_FOR_DELIVERY".equals(target)) {
            order.setExpectedDelivery(Instant.now().plus(event.getExpectedDelivery()));
        }
        orderRepository.save(order);

        StoreOrderResponse storeOrderResponse = toStoreResponse(order);
        ClientOrderResponse orderResponse = toResponse(order);

        kafkaTemplate.send(TopicNames.ORDER_STATUS_CHANGED, String.valueOf(orderId), storeOrderResponse);

        Integer clientId = order.getClient() != null ? order.getClient().getId() : null;
        if (clientId != null) {
            String clientTopic = TopicNames.ORDER_STATUS_CHANGED + "-" + clientId;
            kafkaTemplate.send(clientTopic, String.valueOf(orderId), orderResponse);
        }

        webSocketService.sendOrderUpdate(orderId, orderResponse, storeOrderResponse);
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

    private ClientOrderResponse toResponse(OrderEntity order) {
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
