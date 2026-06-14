package br.ufes.inf.soe.hangry_kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class HungryKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(HungryKafkaApplication.class, args);
    }

}
