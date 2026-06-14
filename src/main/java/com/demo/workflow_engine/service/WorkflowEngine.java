package com.demo.workflow_engine.service;

import com.demo.workflow_engine.dto.*;
import com.demo.workflow_engine.exception.WorkflowNotFoundException;
import com.demo.workflow_engine.model.StepHistory;
import com.demo.workflow_engine.model.WorkflowExecution;
import com.demo.workflow_engine.repository.StepHistoryRepository;
import com.demo.workflow_engine.repository.WorkflowExecutionRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class WorkflowEngine {
    private final WorkflowExecutionRepository executionRepo;
    private final StepHistoryRepository stepRepo;
    private final ObjectMapper mapper = new ObjectMapper();

    private final RetryConfig retryConfig = RetryConfig.custom()
            .maxAttempts(3)
            .waitDuration(Duration.ofMillis(100))
            .build();
    private final Retry retry = Retry.of("stepRetry", retryConfig);

    public Mono<String> triggerWorkflow(WorkflowRequest request) {
        if (request.workflowId() == null || request.steps() == null || request.steps().isEmpty()) {
            return Mono.error(new IllegalArgumentException("Workflow parameters cannot be null or empty."));
        }

        String executionId = UUID.randomUUID().toString();
        log.info("Ingesting workflow trigger request. Assigned ID: [{}], Type: [{}]", executionId, request.workflowId());

        WorkflowExecution exec = WorkflowExecution.builder()
                .id(executionId)
                .workflowId(request.workflowId())
                .status("RUNNING")
                .currentStep(1)
                .contextJson("{}")
                .build();

        return executionRepo.save(exec)
                .doOnSuccess(saved -> {
                    log.info("Workflow execution [{}] saved to H2 store. Offloading execution loop to bounded elastic thread pool.", executionId);
                    executeStepsAsync(saved, request.steps())
                            .subscribeOn(Schedulers.boundedElastic())
                            .subscribe();
                })
                .doOnError(err -> log.error("Failed to persist initial state for workflow execution [{}]", executionId, err))
                .map(WorkflowExecution::getId);
    }

    private Mono<Void> executeStepsAsync(WorkflowExecution exec, List<StepDef> steps) {
        Map<String, Object> globalContext = new ConcurrentHashMap<>();

        return Flux.fromIterable(steps)
                .index()
                .concatMap(tuple -> {
                    int index = tuple.getT1().intValue() + 1;
                    StepDef step = tuple.getT2();

                    return updateProgress(exec.getId(), index)
                            .then(Mono.defer(() -> {
                                log.info("Executing step [{}] ({}/{}) for execution [{}]", step.name(), index, steps.size(), exec.getId());
                                return executeSingleStep(step, globalContext);
                            }))
                            .flatMap(resultMap -> saveStepHistory(exec.getId(), step.name(), "completed", resultMap, index)
                                    .doOnNext(history -> {
                                        globalContext.putAll(resultMap);
                                        log.info("Step [{}] completed successfully for execution [{}]. Context size: {}", step.name(), exec.getId(), globalContext.size());
                                    })
                            );
                })
                .then(completeWorkflow(exec.getId(), "completed"))
                .doOnSuccess(v -> log.info("Workflow execution [{}] finished all steps processing successfully.", exec.getId()))
                .onErrorResume(ex -> {
                    log.error("Fatal exception during workflow execution [{}]. Moving to FAILED status.", exec.getId(), ex);
                    return completeWorkflow(exec.getId(), "failed");
                });
    }

    private Mono<Map<String, Object>> executeSingleStep(StepDef step, Map<String, Object> context) {
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
                .timeout(Duration.ofMillis(500))
                .transformDeferred(io.github.resilience4j.reactor.retry.RetryOperator.of(retry))
                .doOnError(err -> log.warn("Resiliency boundary triggered or timeout reached for step [{}]: {}", step.name(), err.getMessage()));
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
                .switchIfEmpty(Mono.error(new WorkflowNotFoundException("Execution resource not found for ID: " + execId)))
                .map(e -> new ProgressResponse(e.getId(), e.getStatus(), e.getCurrentStep()))
                .doOnNext(res -> log.debug("Progress checked for [{}]: Step {}", execId, res.currentStep()));
    }

    public Mono<ExecutionHistoryResponse> getHistory(String execId) {
        return executionRepo.findById(execId)
                .switchIfEmpty(Mono.error(new WorkflowNotFoundException("Execution history audit logs trace empty or invalid for ID: " + execId)))
                .flatMap(exec -> stepRepo.findByExecutionIdOrderBySequenceOrderAsc(execId)
                        .map(step -> {
                            try {
                                Map<String, Object> out = mapper.readValue(step.getOutputJson(), Map.class);
                                return new StepResultResponse(step.getStepName(), step.getStatus(), out);
                            } catch (Exception ex) {
                                log.error("Failed to parse history JSON payload for step [{}] on execution [{}]", step.getStepName(), execId);
                                return new StepResultResponse(step.getStepName(), step.getStatus(), Map.of());
                            }
                        })
                        .collectList()
                        .map(steps -> new ExecutionHistoryResponse(exec.getId(), exec.getStatus(), steps))
                )
                .doOnSuccess(history -> log.info("Audit log history fully loaded for trace ID: [{}]", execId));
    }
}
