package br.ufes.inf.soe.hungry_kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import br.ufes.inf.soe.hungry_kafka.config.TopicNames;
import br.ufes.inf.soe.hungry_kafka.dto.CreateOrderEvent;

@Service
public class EventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public EventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(CreateOrderEvent event) {
        kafkaTemplate.send(TopicNames.ORDER_EVENTS, String.valueOf(event.clientId()), event);
    }
}
