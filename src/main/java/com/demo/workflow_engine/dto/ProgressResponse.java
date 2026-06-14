package com.demo.workflow_engine.dto;

public record ProgressResponse(String executionId, String status, int currentStep) {
}
