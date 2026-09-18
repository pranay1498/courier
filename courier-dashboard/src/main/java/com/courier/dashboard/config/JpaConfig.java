package com.courier.dashboard.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@EntityScan(basePackages = "com.courier.common.entity")
@EnableJpaRepositories(basePackages = "com.courier.common.repository")
public class JpaConfig {
}
