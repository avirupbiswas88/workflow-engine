package com.demo.workflow_engine.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("STEP_HISTORY")
public class StepHistory {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(StepHistory.class);
    @Id
    private Long id;
    private String executionId;
    private String stepName;
    private String status;
    private String outputJson;
    private int sequenceOrder;
}
