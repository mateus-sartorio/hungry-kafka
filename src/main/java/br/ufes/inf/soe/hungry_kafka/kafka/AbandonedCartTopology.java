package br.ufes.inf.soe.hungry_kafka.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;

import br.ufes.inf.soe.hungry_kafka.config.TopicNames;
import br.ufes.inf.soe.hungry_kafka.dto.AbandonedCartEvent;
import br.ufes.inf.soe.hungry_kafka.dto.AbandonedCartState;
import br.ufes.inf.soe.hungry_kafka.dto.CartAction;
import br.ufes.inf.soe.hungry_kafka.dto.CartEvent;
import br.ufes.inf.soe.hungry_kafka.dto.CreateOrderEvent;

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
        JacksonJsonSerde<CreateOrderEvent> orderSerde = new JacksonJsonSerde<>(CreateOrderEvent.class);
        JacksonJsonSerde<AbandonedCartState> stateSerde = new JacksonJsonSerde<>(AbandonedCartState.class);
        JacksonJsonSerde<AbandonedCartEvent> abandonedSerde = new JacksonJsonSerde<>(AbandonedCartEvent.class);

        // cart-events rekeyed by clientId, then repartitioned to a fixed partition
        // count so it can be co-partitioned with the order stream below.
        KStream<String, CartEvent> carts = builder.stream(
                        TopicNames.CART_EVENTS,
                        Consumed.with(Serdes.String(), cartSerde))
                .filter((String key, CartEvent value) -> value != null && value.clientId() != null && value.productId() != null && value.action() != null)
                .selectKey((String key, CartEvent value) -> String.valueOf(value.clientId()))
                .repartition(Repartitioned.with(Serdes.String(), cartSerde)
                        .withName("abandoned-cart-carts")
                        .withNumberOfPartitions(COPARTITION_PARTITIONS));

        // order events rekeyed by clientId, repartitioned to the same partition count.
        KStream<String, CreateOrderEvent> orders = builder.stream(
                        TopicNames.ORDER_EVENTS,
                        Consumed.with(Serdes.String(), orderSerde))
                .filter((String key, CreateOrderEvent value) -> value != null && value.clientId() != null)
                .selectKey((String key, CreateOrderEvent value) -> String.valueOf(value.clientId()))
                .repartition(Repartitioned.with(Serdes.String(), orderSerde)
                        .withName("abandoned-cart-orders")
                        .withNumberOfPartitions(COPARTITION_PARTITIONS));

        // Both streams are already repartitioned and co-partitioned, so groupByKey
        // adds no further repartition topic.
        KGroupedStream<String, CartEvent> cartsByClient =
                carts.groupByKey(Grouped.with(Serdes.String(), cartSerde));
        KGroupedStream<String, CreateOrderEvent> ordersByClient =
                orders.groupByKey(Grouped.with(Serdes.String(), orderSerde));

        // Co-group carts and orders per client per window into a single state object
        KTable<Windowed<String>, AbandonedCartState> windowed = cartsByClient
                .cogroup((String key, CartEvent cart, AbandonedCartState agg) -> applyCart(agg, cart))
                .cogroup(ordersByClient, (String key, CreateOrderEvent order, AbandonedCartState agg) ->
                        new AbandonedCartState(agg.productIds(), true))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowMinutes)))
                .aggregate(AbandonedCartState::new, Materialized.with(Serdes.String(), stateSerde))
                // Emit one record per window only after the window closes
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        windowed.toStream()
                .peek((Windowed<String> key, AbandonedCartState state) -> System.out.println(String.format(
                        "[Window closed] Client: %s items-left: %s ordered: %s",
                        key.key(), state != null ? state.productIds() : "null", state != null && state.ordered())))
                // Abandoned = items still in cart at window close AND no order placed
                .filter((Windowed<String> key, AbandonedCartState state) -> state != null && !state.ordered() && !state.productIds().isEmpty())
                .map((Windowed<String> key, AbandonedCartState state) -> {
                    Integer clientId = Integer.parseInt(key.key());
                    List<Integer> productIds = new ArrayList<>(state.productIds());
                    System.out.println(String.format("ABANDONED CART DETECTED! Client: %d forgot products: %s", clientId, productIds));
                    return KeyValue.pair(key.key(), new AbandonedCartEvent(clientId, productIds));
                })
                .to(TopicNames.ABANDONED_CART_EVENTS, Produced.with(Serdes.String(), abandonedSerde));
    }

    private static AbandonedCartState applyCart(AbandonedCartState agg, CartEvent cart) {
        if (cart.action() == CartAction.ADDED) {
            agg.productIds().add(cart.productId());
        } else if (cart.action() == CartAction.REMOVED) {
            agg.productIds().remove(cart.productId());
        }
        return agg;
    }
}
