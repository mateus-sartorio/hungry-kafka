package br.ufes.inf.soe.queue_sine.kafka;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaStartup {

    private final Logger logger = LoggerFactory.getLogger(KafkaStartup.class);

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    // Wait up to this many seconds (total) trying to connect
    @Value("${app.kafka.startup.timeout-seconds:30}")
    private int timeoutSeconds;

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        logger.info("Checking Kafka availability at {}", bootstrapServers);

        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);

        try (AdminClient admin = AdminClient.create(config)) {
            while (System.currentTimeMillis() < deadline) {
                try {
                    // This will throw if cluster can't be reached within the timeout
                    admin.describeCluster().nodes().get();
                    logger.info("Kafka is available at {}", bootstrapServers);
                    return;
                } catch (Exception e) {
                    long remaining = (deadline - System.currentTimeMillis()) / 1000L;
                    logger.warn("Kafka not reachable yet ({}s left). Retrying...", remaining);
                    try {
                        Thread.sleep(Duration.ofSeconds(Math.min(5, Math.max(1, remaining))).toMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Failed creating Kafka AdminClient: {}", e.getMessage());
        }

        logger.error("Timed out waiting for Kafka at {} after {} seconds.", bootstrapServers, timeoutSeconds);
    }
}
