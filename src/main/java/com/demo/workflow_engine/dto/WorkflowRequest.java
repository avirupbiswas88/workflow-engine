package com.demo.workflow_engine.dto;

import java.util.List;

public record WorkflowRequest(String workflowId, List<StepDef> steps) {
}
