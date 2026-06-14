package com.demo.workflow_engine.repository;

import com.demo.workflow_engine.model.*;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface WorkflowExecutionRepository extends ReactiveCrudRepository<WorkflowExecution, String> {}