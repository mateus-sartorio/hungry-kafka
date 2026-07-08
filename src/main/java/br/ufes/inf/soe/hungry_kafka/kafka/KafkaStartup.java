package br.ufes.inf.soe.hungry_kafka.kafka;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaStartup {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.startup.max-retries}")
    private int maxRetries;

    @Value("${app.kafka.startup.retry-delay-ms}")
    private long retryDelayMs;

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        IO.println("Checking Kafka availability at " + bootstrapServers);

        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(config)) {
            for (int i = 0; i < maxRetries; i++) {
                try {
                    admin.describeCluster().nodes().get();
                    IO.println("Kafka is available at " + bootstrapServers);
                    return;
                } catch (InterruptedException | ExecutionException e) {
                    Thread.sleep(retryDelayMs);
                }
            }
        } catch (Exception e) {
            IO.println("Failed creating Kafka AdminClient: " + e.getMessage());
        }

        IO.println("Timed out waiting for Kafka at " + bootstrapServers + ".");
    }
}
