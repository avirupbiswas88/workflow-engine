package com.demo.workflow_engine.service;

import com.demo.workflow_engine.dto.*;
import com.demo.workflow_engine.model.*;
import com.demo.workflow_engine.repository.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
public class WorkflowEngine {
    private final WorkflowExecutionRepository executionRepo;
    private final StepHistoryRepository stepRepo;
    private final ObjectMapper mapper = new ObjectMapper();

    // Resiliency Configuration for Step Execution
    private final RetryConfig retryConfig = RetryConfig.custom()
            .maxAttempts(3)
            .waitDuration(Duration.ofMillis(100))
            .build();
    private final Retry retry = Retry.of("stepRetry", retryConfig);

    public Mono<String> triggerWorkflow(WorkflowRequest request) {
        String executionId = UUID.randomUUID().toString();
        
        WorkflowExecution exec = WorkflowExecution.builder()
                .id(executionId)
                .workflowId(request.workflowId())
                .status("RUNNING")
                .currentStep(1)
                .contextJson("{}")
                .build();

        return executionRepo.save(exec)
                .doOnSuccess(saved -> {
                    // Offload actual execution to a non-blocking background thread worker
                    // This allows immediate 202 Accepted response to client
                    executeStepsAsync(saved, request.steps())
                            .subscribeOn(Schedulers.boundedElastic())
                            .subscribe();
                })
                .map(WorkflowExecution::getId);
    }

    private Mono<Void> executeStepsAsync(WorkflowExecution exec, List<StepDef> steps) {
        Map<String, Object> globalContext = new ConcurrentHashMap<>();
        
        return Flux.fromIterable(steps)
                .index() // Track index to check step position
                .concatMap(tuple -> {
                    int index = tuple.getT1().intValue() + 1;
                    StepDef step = tuple.getT2();

                    // 1. Update current execution progress reactively
                    return updateProgress(exec.getId(), index)
                            // 2. Execute business logic with resiliency patterns
                            .then(executeSingleStep(step, globalContext))
                            // 3. Persist individual step result
                            .flatMap(resultMap -> saveStepHistory(exec.getId(), step.name(), "completed", resultMap, index)
                                    .doOnNext(history -> globalContext.putAll(resultMap))
                            );
                })
                .then(completeWorkflow(exec.getId(), "completed"))
                .onErrorResume(ex -> completeWorkflow(exec.getId(), "failed"));
    }

    private Mono<Map<String, Object>> executeSingleStep(StepDef step, Map<String, Object> context) {
        // High TPS Mock Engine: Execute logic based on the step configuration
        return Mono.fromCallable(() -> {
            Map<String, Object> output = new HashMap<>();
            if ("validate".equals(step.name())) {
                output.put("valid", true);
            } else if ("approve".equals(step.name())) {
                output.put("approvedBy", "system");
            } else if ("execute".equals(step.name())) {
                output.put("result", "success");
            }
            return output;
        })
        // Apply Fault-Tolerance Patterns (Retries + Timeout)
        .timeout(Duration.ofMillis(500))
        .transformDeferred(io.github.resilience4j.reactor.retry.RetryOperator.of(retry));
    }

    private Mono<Void> updateProgress(String execId, int stepIndex) {
        return executionRepo.findById(execId)
                .flatMap(exec -> {
                    exec.setCurrentStep(stepIndex);
                    return executionRepo.save(exec);
                }).then();
    }

    private Mono<Void> completeWorkflow(String execId, String finalStatus) {
        return executionRepo.findById(execId)
                .flatMap(exec -> {
                    exec.setStatus(finalStatus);
                    return executionRepo.save(exec);
                }).then();
    }

    private Mono<StepHistory> saveStepHistory(String execId, String name, String status, Map<String, Object> output, int order) {
        try {
            return stepRepo.save(StepHistory.builder()
                    .executionId(execId)
                    .stepName(name)
                    .status(status)
                    .outputJson(mapper.writeValueAsString(output))
                    .sequenceOrder(order)
                    .build());
        } catch (JsonProcessingException e) {
            return Mono.error(e);
        }
    }

    public Mono<ProgressResponse> getProgress(String execId) {
        return executionRepo.findById(execId)
                .map(e -> new ProgressResponse(e.getId(), e.getStatus(), e.getCurrentStep()));
    }

    public Mono<ExecutionHistoryResponse> getHistory(String execId) {
        return executionRepo.findById(execId)
                .flatMap(exec -> stepRepo.findByExecutionIdOrderBySequenceOrderAsc(execId)
                        .map(step -> {
                            try {
                                Map<String, Object> out = mapper.readValue(step.getOutputJson(), Map.class);
                                return new StepResultResponse(step.getStepName(), step.getStatus(), out);
                            } catch (Exception ex) {
                                return new StepResultResponse(step.getStepName(), step.getStatus(), Map.of());
                            }
                        })
                        .collectList()
                        .map(steps -> new ExecutionHistoryResponse(exec.getId(), exec.getStatus(), steps))
                );
    }
}
