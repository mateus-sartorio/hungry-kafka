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

    @Value("${app.abandoned-cart.window-minutes}")
    private long windowMinutes;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        JacksonJsonSerde<CartEvent> cartSerde = new JacksonJsonSerde<>(CartEvent.class);
        JacksonJsonSerde<CreateOrderEvent> orderSerde = new JacksonJsonSerde<>(CreateOrderEvent.class);
        JacksonJsonSerde<AbandonedCartState> stateSerde = new JacksonJsonSerde<>(AbandonedCartState.class);
        JacksonJsonSerde<AbandonedCartEvent> abandonedSerde = new JacksonJsonSerde<>(AbandonedCartEvent.class);

        KStream<String, CartEvent> carts = builder
            .stream(
                        TopicNames.CART_EVENTS,
                        Consumed.with(Serdes.String(), cartSerde)
            )
            .peek((String key, CartEvent value) -> IO.println(String.format("[Topic: cart-events] Received -> Client: %s performed %s on Product: %s", value.clientId(), value.action(), value.productId())))
            .selectKey((String key, CartEvent value) -> String.valueOf(value.clientId()));

        KStream<String, CreateOrderEvent> orders = builder.stream(
                        TopicNames.ORDER_EVENTS,
                        Consumed.with(Serdes.String(), orderSerde))
                .peek((String key, CreateOrderEvent value) -> IO.println(String.format("[Topic: order-events] Received -> Client: %s placed order", value.clientId())))    
                .selectKey((String key, CreateOrderEvent value) -> String.valueOf(value.clientId()));

        KGroupedStream<String, CartEvent> cartsByClient = carts.groupByKey(Grouped.with("abandoned-cart-carts", Serdes.String(), cartSerde));
        KGroupedStream<String, CreateOrderEvent> ordersByClient = orders.groupByKey(Grouped.with("abandoned-cart-orders", Serdes.String(), orderSerde));

        KTable<Windowed<String>, AbandonedCartState> windowed = cartsByClient
                .cogroup((String key, CartEvent cart, AbandonedCartState aggregate) -> applyCart(aggregate, cart))
                .cogroup(ordersByClient, (String key, CreateOrderEvent order, AbandonedCartState aggregate) -> new AbandonedCartState(aggregate.productIds(), true))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowMinutes)))
                .aggregate(AbandonedCartState::new, Materialized.with(Serdes.String(), stateSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        windowed.toStream()
                .peek((Windowed<String> key, AbandonedCartState state) -> IO.println(String.format("[Window closed] Client: %s items-left: %s ordered: %s", key.key(), state.productIds(), state.ordered())))
                .filter((Windowed<String> key, AbandonedCartState state) -> !state.ordered())
                .map((Windowed<String> key, AbandonedCartState state) -> {
                    Integer clientId = Integer.parseInt(key.key());
                    List<Integer> productIds = new ArrayList<>(state.productIds());
                    IO.println(String.format("ABANDONED CART! Client: %d products: %s", clientId, productIds));
                    return KeyValue.pair(key.key(), new AbandonedCartEvent(clientId, productIds));
                })
                .to(TopicNames.ABANDONED_CART_EVENTS, Produced.with(Serdes.String(), abandonedSerde));
    }

    private static AbandonedCartState applyCart(AbandonedCartState aggregate, CartEvent cart) {
        if (cart.action() == CartAction.ADDED) {
            aggregate.productIds().add(cart.productId());
        } else if (cart.action() == CartAction.REMOVED) {
            aggregate.productIds().remove(cart.productId());
        }

        return aggregate;
    }
}
