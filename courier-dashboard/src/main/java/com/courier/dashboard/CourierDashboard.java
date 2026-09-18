package com.courier.dashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackages = "com.courier.common.entity")
@EnableJpaRepositories(basePackages = "com.courier.common.repository")
public class CourierDashboard {
    public static void main(String[] args) {
        SpringApplication.run(CourierDashboard.class, args);
    }
}
