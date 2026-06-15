package br.ufes.inf.soe.hungry_kafka.kafka;

import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaStartup {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        IO.println("Checking Kafka availability at " + bootstrapServers);

        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(config)) {
            for (int i = 0; i < 20; i++) {
                try {
                    admin.describeCluster().nodes().get();
                    IO.println("Kafka is available at " + bootstrapServers);
                    return;
                } catch (Exception e) {
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            IO.println("Failed creating Kafka AdminClient: " + e.getMessage());
        }

        IO.println("Timed out waiting for Kafka at " + bootstrapServers + ".");
    }
}
