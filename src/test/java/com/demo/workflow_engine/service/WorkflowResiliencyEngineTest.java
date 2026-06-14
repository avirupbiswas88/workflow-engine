package com.demo.workflow_engine.service;

import com.demo.workflow_engine.dto.StepDef;
import com.demo.workflow_engine.dto.WorkflowRequest;
import com.demo.workflow_engine.model.WorkflowExecution;
import com.demo.workflow_engine.repository.StepHistoryRepository;
import com.demo.workflow_engine.repository.WorkflowExecutionRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class WorkflowResiliencyEngineTest {

    @Test
    @DisplayName("Resiliency Test: Unresponsive downstream operations trigger timeout limits safely")
    void testWorkflowStepTimeoutHandling() {
        // Arrange
        WorkflowExecutionRepository executionRepo = Mockito.mock(WorkflowExecutionRepository.class);
        StepHistoryRepository stepRepo = Mockito.mock(StepHistoryRepository.class);
        WorkflowEngine workflowEngine = new WorkflowEngine(executionRepo, stepRepo);

        // Slow step simulation that exceeds our 500ms timeout parameter limit boundary
        WorkflowRequest request = new WorkflowRequest("slow-flow", 
                List.of(new StepDef("unresponsive-step-name", Map.of())));

        WorkflowExecution initialExec = WorkflowExecution.builder()
                .id("test-timeout-id")
                .workflowId("slow-flow")
                .status("RUNNING")
                .currentStep(1)
                .build();

        when(executionRepo.save(any(WorkflowExecution.class))).thenReturn(Mono.just(initialExec));
        when(executionRepo.findById("test-timeout-id")).thenReturn(Mono.just(initialExec));

        // Act & Assert
        Mono<String> executionIdMono = workflowEngine.triggerWorkflow(request);

        StepVerifier.create(executionIdMono)
                .expectNext("test-timeout-id")
                .verifyComplete();

        // Allow background thread worker window to process the step and hit the timeout parameter limit
        // Since executeSingleStep handles 'unresponsive-step-name' via fallback maps or errors, 
        // the workflow engine marks state as 'failed' on unhandled timeout exception states.
        verify(executionRepo, timeout(1000).atLeastOnce()).save(argThat(exec -> 
             "failed".equals(exec.getStatus())
        ));
    }
}
