package com.demo.workflow_engine.dto;

import java.util.Map;

public record StepResultResponse(String name, String status, Map<String, Object> output) {
}
