package br.ufes.inf.soe.queue_sine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class QueueSineApplication {

    public static void main(String[] args) {
        SpringApplication.run(QueueSineApplication.class, args);
    }

}
