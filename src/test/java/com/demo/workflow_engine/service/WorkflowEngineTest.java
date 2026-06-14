package com.demo.workflow_engine.service;

import com.demo.workflow_engine.dto.StepDef;
import com.demo.workflow_engine.dto.WorkflowRequest;
import com.demo.workflow_engine.exception.WorkflowNotFoundException;
import com.demo.workflow_engine.model.WorkflowExecution;
import com.demo.workflow_engine.repository.StepHistoryRepository;
import com.demo.workflow_engine.repository.WorkflowExecutionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class WorkflowEngineTest {

    private WorkflowExecutionRepository executionRepo;
    private StepHistoryRepository stepRepo;
    private WorkflowEngine workflowEngine;

    @BeforeEach
    void setUp() {
        executionRepo = Mockito.mock(WorkflowExecutionRepository.class);
        stepRepo = Mockito.mock(StepHistoryRepository.class);
        workflowEngine = new WorkflowEngine(executionRepo, stepRepo);
    }

    @Test
    @DisplayName("Should successfully trigger a valid workflow and return execution ID")
    void testTriggerWorkflowSuccess() {
        // Arrange
        WorkflowRequest request = new WorkflowRequest("contract-review",
                List.of(new StepDef("validate", Map.of("contractId", "123"))));

        // 1. Mock the initial save behavior
        when(executionRepo.save(any(WorkflowExecution.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        // 2. FIX: Mock findById to return an empty Mono (or a mock WorkflowExecution object)
        // to prevent the asynchronous background loop from crashing with a NullPointerException
        when(executionRepo.findById(any(String.class)))
                .thenAnswer(invocation -> {
                    String id = invocation.getArgument(0);
                    return Mono.just(WorkflowExecution.builder()
                            .id(id)
                            .workflowId("contract-review")
                            .status("RUNNING")
                            .currentStep(1)
                            .contextJson("{}")
                            .build());
                });

        // 3. Mock the step tracking persistence execution to prevent downstream null loops
        when(stepRepo.save(any())).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        // Act & Assert
        Mono<String> result = workflowEngine.triggerWorkflow(request);

        StepVerifier.create(result)
                .expectNextMatches(id -> id != null && !id.isEmpty())
                .verifyComplete();
    }

    @Test
    @DisplayName("Corner Case: Null or Empty parameter payloads should emit IllegalArgumentException")
    void testTriggerWorkflowCornerCaseEmptySteps() {
        // Arrange
        WorkflowRequest requestInvalid = new WorkflowRequest("contract-review", Collections.emptyList());

        // Act & Assert
        Mono<String> result = workflowEngine.triggerWorkflow(requestInvalid);

        StepVerifier.create(result)
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    @DisplayName("Corner Case: Fetching progress of non-existent execution ID throws custom Exception")
    void testGetProgressNotFound() {
        // Arrange
        String missingId = "missing-uuid-123";
        when(executionRepo.findById(missingId)).thenReturn(Mono.empty());

        // Act & Assert
        StepVerifier.create(workflowEngine.getProgress(missingId))
                .expectError(WorkflowNotFoundException.class)
                .verify();
    }
}
