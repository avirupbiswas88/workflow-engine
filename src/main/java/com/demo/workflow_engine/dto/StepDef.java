package com.demo.workflow_engine.dto;

import java.util.Map;

public record StepDef(String name, Map<String, Object> input) {
}
