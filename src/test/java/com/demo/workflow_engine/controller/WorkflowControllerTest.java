package com.demo.workflow_engine.controller;

import com.demo.workflow_engine.dto.ProgressResponse;
import com.demo.workflow_engine.dto.StepDef;
import com.demo.workflow_engine.dto.WorkflowRequest;
import com.demo.workflow_engine.exception.WorkflowNotFoundException;
import com.demo.workflow_engine.service.WorkflowEngine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@WebFluxTest(controllers = WorkflowController.class)
class WorkflowControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private WorkflowEngine engine;

    @Test
    @DisplayName("POST /workflows/execute should accept payloads and map 202 status code")
    void testExecuteWorkflowEndpointSuccess() {
        // Arrange
        WorkflowRequest request = new WorkflowRequest("contract-review", 
                List.of(new StepDef("validate", Map.of("contractId", "123"))));
        
        when(engine.triggerWorkflow(any(WorkflowRequest.class)))
                .thenReturn(Mono.just("mocked-uuid-999"));

        // Act & Assert
        webTestClient.post()
                .uri("/workflows/execute")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.executionId").isEqualTo("mocked-uuid-999");
    }

    @Test
    @DisplayName("Corner Case: GET Progress returns HTTP 404 cleanly when domain error fires")
    void testGetProgressEndpointCornerCaseNotFound() {
        // Arrange
        String executionId = "unknown-id";
        when(engine.getProgress(executionId))
                .thenReturn(Mono.error(new WorkflowNotFoundException("Execution resource not found for ID: " + executionId)));

        // Act & Assert
        webTestClient.get()
                .uri("/workflows/executions/{id}/progress", executionId)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                // Extract the response body as a String and assert with AssertJ
                .consumeWith(response -> {
                    String body = new String(response.getResponseBody());
                    org.assertj.core.api.Assertions.assertThat(body)
                            .contains("Execution resource not found");
                });

    }

    @Test
    @DisplayName("Corner Case: GET Progress should render metrics with 200 OK when entity is processing")
    void testGetProgressEndpointSuccess() {
        // Arrange
        String executionId = "valid-active-id";
        when(engine.getProgress(executionId))
                .thenReturn(Mono.just(new ProgressResponse(executionId, "RUNNING", 2)));

        // Act & Assert
        webTestClient.get()
                .uri("/workflows/executions/{id}/progress", executionId)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("RUNNING")
                .jsonPath("$.currentStep").isEqualTo(2);
    }
}
