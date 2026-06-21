package br.ufes.inf.soe.hungry_kafka.kafka;

import br.ufes.inf.soe.hungry_kafka.config.TopicNames;
import br.ufes.inf.soe.hungry_kafka.dto.AbandonedCartEvent;
import br.ufes.inf.soe.hungry_kafka.dto.AbandonedCartState;
import br.ufes.inf.soe.hungry_kafka.dto.CartAction;
import br.ufes.inf.soe.hungry_kafka.dto.CartEvent;
import br.ufes.inf.soe.hungry_kafka.dto.CreateOrderRequest;
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
 * Situation A — Abandoned Cart (absence pattern).
 *
 * <p>A client adds items to the cart but does not place an order within a time
 * window. This is detected by co-grouping {@code cart-events} and {@code order}
 * by {@code clientId} into a tumbling window: the cart aggregate remembers which
 * products are still in the cart and whether an order arrived. The window result
 * is held back with {@code suppress(untilWindowCloses)} so a single record is
 * emitted only when the window finally closes; if it still holds items and no
 * order was seen, an {@link AbandonedCartEvent} is produced.
 *
 * <p>Stateless ops: filter, selectKey, map. Stateful ops: cogroup, windowedBy,
 * aggregate, suppress.
 *
 * <p>Note: a window only closes once stream time advances past its end, which
 * requires later records to arrive on the input topics. With no further traffic
 * the alert will not fire until the next event pushes stream time forward — an
 * inherent trait of windowed suppression rather than a wall-clock timer.
 */
@Component
public class AbandonedCartTopology {

    @Value("${app.abandoned-cart.window-minutes:15}")
    private long windowMinutes;

    /**
     * cart-events (6 partitions) and order (3 partitions) are not co-partitioned,
     * which a cogroup forbids. Both streams are rekeyed by clientId and therefore
     * must be repartitioned anyway, so we pin both repartition topics to the same
     * partition count to make them co-partitioned.
     */
    private static final int COPARTITION_PARTITIONS = 6;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        JacksonJsonSerde<CartEvent> cartSerde = new JacksonJsonSerde<>(CartEvent.class);
        JacksonJsonSerde<CreateOrderRequest> orderSerde = new JacksonJsonSerde<>(CreateOrderRequest.class);
        JacksonJsonSerde<AbandonedCartState> stateSerde = new JacksonJsonSerde<>(AbandonedCartState.class);
        JacksonJsonSerde<AbandonedCartEvent> abandonedSerde = new JacksonJsonSerde<>(AbandonedCartEvent.class);

        // cart-events rekeyed by clientId, then repartitioned to a fixed partition
        // count so it can be co-partitioned with the order stream below.
        KStream<String, CartEvent> carts = builder.stream(
                        TopicNames.CART_EVENTS,
                        Consumed.with(Serdes.String(), cartSerde))
                .filter((String key, CartEvent value) -> value != null && value.getClientId() != null && value.getProductId() != null && value.getAction() != null)
                .selectKey((String key, CartEvent value) -> String.valueOf(value.getClientId()))
                .repartition(Repartitioned.with(Serdes.String(), cartSerde)
                        .withName("abandoned-cart-carts")
                        .withNumberOfPartitions(COPARTITION_PARTITIONS));

        // order events rekeyed by clientId, repartitioned to the same partition count.
        KStream<String, CreateOrderRequest> orders = builder.stream(
                        TopicNames.ORDER_EVENTS,
                        Consumed.with(Serdes.String(), orderSerde))
                .filter((String key, CreateOrderRequest value) -> value != null && value.getClientId() != null)
                .selectKey((String key, CreateOrderRequest value) -> String.valueOf(value.getClientId()))
                .repartition(Repartitioned.with(Serdes.String(), orderSerde)
                        .withName("abandoned-cart-orders")
                        .withNumberOfPartitions(COPARTITION_PARTITIONS));

        // Both streams are already repartitioned and co-partitioned, so groupByKey
        // adds no further repartition topic.
        KGroupedStream<String, CartEvent> cartsByClient =
                carts.groupByKey(Grouped.with(Serdes.String(), cartSerde));
        KGroupedStream<String, CreateOrderRequest> ordersByClient =
                orders.groupByKey(Grouped.with(Serdes.String(), orderSerde));

        // Co-group carts and orders per client per window into a single state object
        KTable<Windowed<String>, AbandonedCartState> windowed = cartsByClient
                .cogroup((String key, CartEvent cart, AbandonedCartState agg) -> applyCart(agg, cart))
                .cogroup(ordersByClient, (String key, CreateOrderRequest order, AbandonedCartState agg) -> {
                    agg.setOrdered(true);
                    return agg;
                })
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowMinutes)))
                .aggregate(AbandonedCartState::new, Materialized.with(Serdes.String(), stateSerde))
                // Emit one record per window only after the window closes
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        windowed.toStream()
                .peek((Windowed<String> key, AbandonedCartState state) -> System.out.println(String.format(
                        "[Window closed] Client: %s items-left: %s ordered: %s",
                        key.key(), state != null ? state.getProductIds() : "null", state != null && state.isOrdered())))
                // Abandoned = items still in cart at window close AND no order placed
                .filter((Windowed<String> key, AbandonedCartState state) -> state != null && !state.isOrdered() && !state.getProductIds().isEmpty())
                .map((Windowed<String> key, AbandonedCartState state) -> {
                    Integer clientId = Integer.parseInt(key.key());
                    List<Integer> productIds = new ArrayList<>(state.getProductIds());
                    System.out.println(String.format("ABANDONED CART DETECTED! Client: %d forgot products: %s", clientId, productIds));
                    return KeyValue.pair(key.key(), new AbandonedCartEvent(clientId, productIds));
                })
                .to(TopicNames.ABANDONED_CART_EVENTS, Produced.with(Serdes.String(), abandonedSerde));
    }

    private static AbandonedCartState applyCart(AbandonedCartState agg, CartEvent cart) {
        if (cart.getAction() == CartAction.ADDED) {
            agg.getProductIds().add(cart.getProductId());
        } else if (cart.getAction() == CartAction.REMOVED) {
            agg.getProductIds().remove(cart.getProductId());
        }
        return agg;
    }
}
