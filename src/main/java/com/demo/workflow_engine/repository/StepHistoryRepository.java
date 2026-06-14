package com.demo.workflow_engine.repository;

import com.demo.workflow_engine.model.StepHistory;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface StepHistoryRepository extends ReactiveCrudRepository<StepHistory, Long> {
    Flux<StepHistory> findByExecutionIdOrderBySequenceOrderAsc(String executionId);
}