package com.demo.workflow_engine.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI workflowEngineOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("High-Throughput Workflow Execution Service")
                        .description("Reactive, non-blocking event-driven engine capable of executing complex multi-step workflows at scale (10k+ TPS targets).")
                        .version("v1.0.0")
                        .contact(new Contact()
                                .name("Architecture Team")
                                .email("arch-team@enterprise.com"))
                        .license(new License()
                                .name("Apache 2.0")
                                .url("https://springdoc.org")));
    }
}
