package com.demo.workflow_engine.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.support.WebExchangeBindException;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    // Target custom domain exception for execution mismatches
    @ExceptionHandler(WorkflowNotFoundException.class)
    public Mono<ResponseEntity<Map<String, Object>>> handleNotFound(WorkflowNotFoundException ex) {
        log.warn("Workflow resource lookup failed: {}", ex.getMessage());
        return Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND).body(createErrorBody(HttpStatus.NOT_FOUND, ex.getMessage())));
    }

    // Capture validation errors for dynamic structural schema binding payloads
    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<ResponseEntity<Map<String, Object>>> handleValidation(WebExchangeBindException ex) {
        log.warn("Payload structural verification failed: {}", ex.getReason());
        return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST).body(createErrorBody(HttpStatus.BAD_REQUEST, "Malformed JSON request schema input.")));
    }

    // Catch-all fallback for internal processing crashes
    @ExceptionHandler(Exception.class)
    public Mono<ResponseEntity<Map<String, Object>>> handleGenericError(Exception ex) {
        log.error("Fatal system variance caught in reactive controller pipeline", ex);
        return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(createErrorBody(HttpStatus.INTERNAL_SERVER_ERROR, "An internal server variance occurred during processing.")));
    }

    private Map<String, Object> createErrorBody(HttpStatus status, String message) {
        return Map.of(
                "timestamp", Instant.now().toString(),
                "status", status.value(),
                "error", status.getReasonPhrase(),
                "message", message
        );
    }
}
