package br.ufes.inf.soe.hungry_kafka.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import javax.sql.DataSource;

@Slf4j
@Component
@RequiredArgsConstructor
public class DatabaseInitializer {

    private final DataSource dataSource;

    @EventListener(ApplicationReadyEvent.class)
    public void initializeDatabase() {
        try {
            log.info("=== Starting Flyway Database Migrations ===");
            Flyway flyway = Flyway.configure()
                    .dataSource(dataSource)
                    .locations("classpath:db/migration")
                    .load();
            
                var migrateResult = flyway.migrate();
                log.info("=== Flyway Migration Complete: {} migrations executed ===", migrateResult.migrationsExecuted);
        } catch (Exception e) {
            log.error("Error initializing database with Flyway", e);
            throw new RuntimeException(e);
        }
    }
}
