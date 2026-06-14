package com.demo.workflow_engine.controller;

import com.demo.workflow_engine.dto.ExecutionHistoryResponse;
import com.demo.workflow_engine.dto.ProgressResponse;
import com.demo.workflow_engine.dto.WorkflowRequest;
import com.demo.workflow_engine.service.WorkflowEngine;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import java.util.Map;

@RestController
@RequestMapping("/workflows")
@RequiredArgsConstructor
@Tag(name = "Workflow Operations", description = "Endpoints for initiating workflows and tracking runtime execution telemetry.")
public class WorkflowController {
    
    private final WorkflowEngine engine;

    @PostMapping("/execute")
    @Operation(
        summary = "Submit a workflow for execution",
        description = "Ingests a structured sequence of workflow steps, returns immediately with an execution ticket, and dispatches tasks asynchronously using non-blocking worker threads.",
        responses = {
            @ApiResponse(responseCode = "202", description = "Workflow execution accepted", 
                         content = @Content(schema = @Schema(example = "{\"executionId\": \"b6a84c8a-7e3e-4b68-b733-149fa8619bc9\"}"))),
            @ApiResponse(responseCode = "400", description = "Invalid payload structural schema")
        }
    )
    public Mono<ResponseEntity<Map<String, String>>> executeWorkflow(@RequestBody WorkflowRequest request) {
        return engine.triggerWorkflow(request)
                .map(id -> ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("executionId", id)));
    }

    @GetMapping("/executions/{id}/progress")
    @Operation(
        summary = "Fetch running execution metrics",
        description = "Retrieves live data on the workflow execution, including the current state machine phase and step pointers.",
        responses = {
            @ApiResponse(responseCode = "200", description = "Progress retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Execution ID instance not found")
        }
    )
    public Mono<ProgressResponse> getProgress(
            @PathVariable @Parameter(description = "Unique workflow execution UUID identifier") String id) {
        return engine.getProgress(id);
    }

    @GetMapping("/executions/{id}/history")
    @Operation(
        summary = "Fetch step execution audit logs",
        description = "Returns complete structured historical auditing traces for a given workflow execution instance.",
        responses = {
            @ApiResponse(responseCode = "200", description = "Audit trail loaded successfully"),
            @ApiResponse(responseCode = "404", description = "Execution ID instance not found")
        }
    )
    public Mono<ExecutionHistoryResponse> getHistory(
            @PathVariable @Parameter(description = "Unique workflow execution UUID identifier") String id) {
        return engine.getHistory(id);
    }
}
