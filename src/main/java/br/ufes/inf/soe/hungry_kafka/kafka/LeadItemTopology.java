package br.ufes.inf.soe.hungry_kafka.kafka;

import br.ufes.inf.soe.hungry_kafka.config.TopicNames;
import br.ufes.inf.soe.hungry_kafka.dto.CartAction;
import br.ufes.inf.soe.hungry_kafka.dto.CartEvent;
import br.ufes.inf.soe.hungry_kafka.dto.ItemViewEvent;
import br.ufes.inf.soe.hungry_kafka.dto.LeadItemEvent;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class LeadItemTopology {

    @Value("${app.hot-lead.join-window-minutes:5}")
    private long joinWindowMinutes;

    @Value("${app.hot-lead.view-threshold:5}")
    private long viewThreshold;

    @Value("${app.hot-item.duration-seconds:15}")
    private long countWindowSeconds;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        JacksonJsonSerde<ItemViewEvent> viewSerde = new JacksonJsonSerde<>(ItemViewEvent.class);
        JacksonJsonSerde<CartEvent> cartSerde = new JacksonJsonSerde<>(CartEvent.class);
        JacksonJsonSerde<LeadItemEvent> leadItemSerde = new JacksonJsonSerde<>(LeadItemEvent.class);

        KStream<String, ItemViewEvent> views = builder.stream(
                TopicNames.ITEM_VIEW_EVENTS,
                Consumed.with(Serdes.String(), viewSerde)
        ).peek((String key, ItemViewEvent value) -> System.out.println(String.format("[Topic: item-view-events] Received -> Client: %s viewed Product: %s", value != null ? value.getClientId() : "null", value != null ? value.getProductId() : "null")));

        KStream<String, CartEvent> carts = builder.stream(
                TopicNames.CART_EVENTS,
                Consumed.with(Serdes.String(), cartSerde)
        ).peek((String key, CartEvent value) -> System.out.println(String.format("[Topic: cart-events] Received -> Client: %s performed %s on Product: %s", value != null ? value.getClientId() : "null", value != null ? value.getAction() : "null", value != null ? value.getProductId() : "null")));

        // Rekey views to clientId_productId
        KStream<String, ItemViewEvent> viewsRekeyed = views
                .filter((String key, ItemViewEvent value) -> value != null && value.getClientId() != null && value.getProductId() != null)
                .selectKey((String key, ItemViewEvent value) -> value.getClientId() + "_" + value.getProductId());

        // Rekey carts to clientId_productId and filter only ADD actions
        KStream<String, CartEvent> cartsRekeyed = carts
                .filter((String key, CartEvent value) -> value != null && value.getAction() == CartAction.ADDED && value.getClientId() != null && value.getProductId() != null)
                .selectKey((String key, CartEvent value) -> value.getClientId() + "_" + value.getProductId());

        // Count views in a Tumbling window of 5 minutes
        KStream<String, Long> frequentViews = viewsRekeyed
                .groupByKey(Grouped.with(Serdes.String(), viewSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .count()
                .toStream()
                .peek((key, count) -> {
                    String[] parts = key.key().split("_");
                    System.out.println(String.format("[Aggregation] Client: %s viewed Product: %s exactly %d times in this window", parts[0], parts[1], count));
                })
                .filter((Windowed<String> key, Long count) -> count >= viewThreshold)
                .map((Windowed<String> key, Long value) -> KeyValue.pair(key.key(), value));

        // Join frequentViews and carts within a 5 minute window
        KStream<String, String> joinedStream = frequentViews.join(
                cartsRekeyed,
                (Long count, CartEvent cart) -> "HOT_LEAD",
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(joinWindowMinutes)),
                StreamJoined.with(Serdes.String(), Serdes.Long(), cartSerde)
        );

        // Map to LeadItemEvent and send to topic
        joinedStream.map((String key, String value) -> {
            String[] parts = key.split("_");
            Integer clientId = Integer.parseInt(parts[0]);
            Integer productId = Integer.parseInt(parts[1]);
            System.out.println(String.format("HOT LEAD DETECTED! Client: %d Product: %d", clientId, productId));
            return KeyValue.pair(String.valueOf(clientId), new LeadItemEvent(clientId, productId));
        }).to(TopicNames.LEAD_ITEM_EVENTS, Produced.with(Serdes.String(), leadItemSerde));
    }
}
