package br.ufes.inf.soe.hungry_kafka.kafka;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
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
import br.ufes.inf.soe.hungry_kafka.dto.ItemViewEvent;
import br.ufes.inf.soe.hungry_kafka.dto.LeadItemEvent;

@Component
public class LeadItemTopology {

    @Value("${app.hot-lead.view-window-minutes}")
    private long viewWindowMinutes;

    @Value("${app.hot-lead.view-threshold}")
    private long viewThreshold;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        JacksonJsonSerde<ItemViewEvent> viewSerde = new JacksonJsonSerde<>(ItemViewEvent.class);
        JacksonJsonSerde<CartEvent> cartSerde = new JacksonJsonSerde<>(CartEvent.class);
        JacksonJsonSerde<LeadItemEvent> leadItemSerde = new JacksonJsonSerde<>(LeadItemEvent.class);

        KStream<String, ItemViewEvent> views = builder
                .stream(
                        TopicNames.ITEM_VIEW_EVENTS,
                        Consumed.with(Serdes.String(), viewSerde)
                )
                .peek((String key, ItemViewEvent value) -> IO.println(String.format("[LeadItemTopology] Received -> Client: %s viewed Product: %s", value.clientId(), value.productId())))
                .selectKey((String key, ItemViewEvent value) -> value.clientId() + "_" + value.productId());

        KStream<String, CartEvent> carts = builder
                .stream(
                        TopicNames.CART_EVENTS,
                        Consumed.with(Serdes.String(), cartSerde)
                )
                .peek((String key, CartEvent value) -> IO.println(String.format("[LeadItemTopology] Received -> Client: %s performed %s on Product: %s", value.clientId(), value.action(), value.productId())))
                .filter((String key, CartEvent value) -> value.action() == CartAction.ADDED)
                .selectKey((String key, CartEvent value) -> value.clientId() + "_" + value.productId());

        KTable<String, Long> recentViewCounts = views
                .groupByKey(Grouped.with(Serdes.String(), viewSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(viewWindowMinutes)))
                .count()
                .toStream()
                .peek((Windowed<String> key, Long count) -> {
                    String[] parts = key.key().split("_");
                    IO.println(String.format("[LeadItemTopology] Client: %s viewed Product: %s exactly %d times in this window", parts[0], parts[1], count));
                })
                .selectKey((key, count) -> key.key())
                .toTable(Materialized.with(Serdes.String(), Serdes.Long()));

        carts
                .join(
                        recentViewCounts,
                        (CartEvent cart, Long viewCount) -> viewCount,
                        Joined.with(Serdes.String(), cartSerde, Serdes.Long())
                )
                .filter((String key, Long viewCount) -> viewCount >= viewThreshold)
                .map((String key, Long viewCount) -> {
                    String[] parts = key.split("_");
                    Integer clientId = Integer.valueOf(parts[0]);
                    Integer productId = Integer.valueOf(parts[1]);
                    IO.println(String.format("[LeadItemTopology] HOT LEAD DETECTED! Client: %d Product: %d", clientId, productId));
                    return KeyValue.pair(String.valueOf(clientId), new LeadItemEvent(clientId, productId));
                })
                .to(TopicNames.LEAD_ITEM_EVENTS, Produced.with(Serdes.String(), leadItemSerde));
    }
}