package br.ufes.inf.soe.hungry_kafka.kafka;

import br.ufes.inf.soe.hungry_kafka.config.TopicNames;
import br.ufes.inf.soe.hungry_kafka.dto.CartAction;
import br.ufes.inf.soe.hungry_kafka.dto.CartEvent;
import br.ufes.inf.soe.hungry_kafka.dto.CreateOrderRequest;
import br.ufes.inf.soe.hungry_kafka.dto.HotItemEvent;
import br.ufes.inf.soe.hungry_kafka.dto.ItemViewEvent;
import br.ufes.inf.soe.hungry_kafka.dto.OrderItemInput;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Hot Item (popularity pattern).
 *
 * <p>Counts every interaction with a product — a view, a cart-add, or a purchase —
 * across <em>all</em> clients within a tumbling window. The three input streams are
 * normalised to a common {@code (productId -> interaction-type)} shape, merged into a
 * single stream, and counted per product per window. When a single product reaches
 * {@code threshold} interactions inside the window it is flagged as "hot" and a
 * {@link HotItemEvent} is produced, so every client and the store can be notified.
 *
 * <p>Stateless ops: filter, map, flatMap, merge. Stateful ops: windowedBy, count.
 *
 * <p>A purchase counts each order line once (regardless of its quantity); change the
 * {@code purchases} flatMap to expand by quantity if you want units to count individually.
 */
@Component
public class HotItemTopology {

    @Value("${app.hot-item.window-minutes:10}")
    private long windowMinutes;

    @Value("${app.hot-item.threshold:10}")
    private long threshold;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        JacksonJsonSerde<ItemViewEvent> viewSerde = new JacksonJsonSerde<>(ItemViewEvent.class);
        JacksonJsonSerde<CartEvent> cartSerde = new JacksonJsonSerde<>(CartEvent.class);
        JacksonJsonSerde<CreateOrderRequest> orderSerde = new JacksonJsonSerde<>(CreateOrderRequest.class);
        JacksonJsonSerde<HotItemEvent> hotItemSerde = new JacksonJsonSerde<>(HotItemEvent.class);

        // A view = one interaction, keyed by productId.
        KStream<String, String> views = builder.stream(
                        TopicNames.ITEM_VIEW_EVENTS,
                        Consumed.with(Serdes.String(), viewSerde))
                .filter((String key, ItemViewEvent value) -> value != null && value.getProductId() != null)
                .map((String key, ItemViewEvent value) -> KeyValue.pair(String.valueOf(value.getProductId()), "VIEW"));

        // A cart ADD = one interaction, keyed by productId (REMOVED is ignored).
        KStream<String, String> cartAdds = builder.stream(
                        TopicNames.CART_EVENTS,
                        Consumed.with(Serdes.String(), cartSerde))
                .filter((String key, CartEvent value) -> value != null && value.getAction() == CartAction.ADDED && value.getProductId() != null)
                .map((String key, CartEvent value) -> KeyValue.pair(String.valueOf(value.getProductId()), "CART"));

        // A purchase = one interaction per order line, keyed by productId.
        KStream<String, String> purchases = builder.stream(
                        TopicNames.ORDER_EVENTS,
                        Consumed.with(Serdes.String(), orderSerde))
                .filter((String key, CreateOrderRequest value) -> value != null && value.getItems() != null)
                .flatMap((String key, CreateOrderRequest order) -> {
                    List<KeyValue<String, String>> interactions = new ArrayList<>();
                    for (OrderItemInput item : order.getItems()) {
                        if (item != null && item.getProductId() != null) {
                            interactions.add(KeyValue.pair(String.valueOf(item.getProductId()), "ORDER"));
                        }
                    }
                    return interactions;
                });

        // Merge the three interaction streams and count per product in a tumbling window.
        KStream<String, HotItemEvent> hotItems = views
                .merge(cartAdds)
                .merge(purchases)
                .peek((String productId, String type) -> System.out.println(
                        String.format("[hot-item] interaction %s on Product: %s", type, productId)))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowMinutes)))
                .count()
                .toStream()
                .peek((Windowed<String> key, Long count) -> System.out.println(
                        String.format("[hot-item] Product: %s has %d interactions in this window", key.key(), count)))
                .filter((Windowed<String> key, Long count) -> count >= threshold)
                .map((Windowed<String> key, Long count) -> {
                    Integer productId = Integer.parseInt(key.key());
                    System.out.println(String.format("HOT ITEM DETECTED! Product: %d (%d interactions in the last %d min)", productId, count, windowMinutes));
                    return KeyValue.pair(key.key(), new HotItemEvent(productId));
                });

        hotItems.to(TopicNames.HOT_ITEM_EVENTS, Produced.with(Serdes.String(), hotItemSerde));
    }
}
