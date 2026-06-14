package com.demo.workflow_engine.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Table;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("WORKFLOW_EXECUTION")
public class WorkflowExecution {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(WorkflowExecution.class);
    @Id
    private String id;
    private String workflowId;
    private String status; // RUNNING, COMPLETED, FAILED
    private int currentStep;
    private String contextJson; // Marshaled Map<String, Object>
    @Version
    private int version; // Optimistic locking for scale
}
