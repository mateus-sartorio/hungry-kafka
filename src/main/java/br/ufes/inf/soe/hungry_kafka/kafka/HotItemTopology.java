package br.ufes.inf.soe.hungry_kafka.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;

import br.ufes.inf.soe.hungry_kafka.config.TopicNames;
import br.ufes.inf.soe.hungry_kafka.dto.CartAction;
import br.ufes.inf.soe.hungry_kafka.dto.CartEvent;
import br.ufes.inf.soe.hungry_kafka.dto.CreateOrderEvent;
import br.ufes.inf.soe.hungry_kafka.dto.HotItemEvent;
import br.ufes.inf.soe.hungry_kafka.dto.ItemViewEvent;
import br.ufes.inf.soe.hungry_kafka.dto.OrderItemInput;

@Component
public class HotItemTopology {

    @Value("${app.hot-item.window-minutes}")
    private long windowMinutes;

    @Value("${app.hot-item.threshold}")
    private long threshold;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        JacksonJsonSerde<ItemViewEvent> viewSerde = new JacksonJsonSerde<>(ItemViewEvent.class);
        JacksonJsonSerde<CartEvent> cartSerde = new JacksonJsonSerde<>(CartEvent.class);
        JacksonJsonSerde<CreateOrderEvent> orderSerde = new JacksonJsonSerde<>(CreateOrderEvent.class);
        JacksonJsonSerde<HotItemEvent> hotItemSerde = new JacksonJsonSerde<>(HotItemEvent.class);

        KStream<String, String> views = builder.stream(
                        TopicNames.ITEM_VIEW_EVENTS,
                        Consumed.with(Serdes.String(), viewSerde))
                .map((String key, ItemViewEvent value) -> KeyValue.pair(String.valueOf(value.productId()), "VIEW"));

        KStream<String, String> cartAdds = builder.stream(
                        TopicNames.CART_EVENTS,
                        Consumed.with(Serdes.String(), cartSerde))
                .filter((String key, CartEvent value) -> value.action() == CartAction.ADDED)
                .map((String key, CartEvent value) -> KeyValue.pair(String.valueOf(value.productId()), "CART"));

        KStream<String, String> purchases = builder.stream(
                        TopicNames.ORDER_EVENTS,
                        Consumed.with(Serdes.String(), orderSerde))
                .flatMap((String key, CreateOrderEvent order) -> {
                    List<KeyValue<String, String>> interactions = new ArrayList<>();

                    for (OrderItemInput item : order.items()) {
                        interactions.add(KeyValue.pair(String.valueOf(item.productId()), "ORDER"));
                    }

                    return interactions;
                });

        KStream<String, HotItemEvent> hotItems = views
                .merge(cartAdds)
                .merge(purchases)
                .peek((String productId, String type) -> IO.println(String.format("[HotItemTopology] Interaction %s on Product: %s", type, productId)))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowMinutes)))
                .count()
                .toStream()
                .peek((Windowed<String> key, Long count) -> IO.println(String.format("[HotItemTopology] Product: %s has %d interactions in this window", key.key(), count)))
                .filter((Windowed<String> key, Long count) -> count >= threshold)
                .map((Windowed<String> key, Long count) -> {
                    Integer productId = Integer.valueOf(key.key());
                    IO.println(String.format("[HotItemTopology] HOT ITEM DETECTED! Product: %d (%d interactions in the last %d min)", productId, count, windowMinutes));
                    return KeyValue.pair(key.key(), new HotItemEvent(productId));
                });

        hotItems.to(TopicNames.HOT_ITEM_EVENTS, Produced.with(Serdes.String(), hotItemSerde));
    }
}
