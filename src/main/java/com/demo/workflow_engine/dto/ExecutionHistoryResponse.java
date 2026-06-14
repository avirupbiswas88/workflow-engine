package com.demo.workflow_engine.dto;

import java.util.List;

public record ExecutionHistoryResponse(String executionId, String status, List<StepResultResponse> steps) {
}
