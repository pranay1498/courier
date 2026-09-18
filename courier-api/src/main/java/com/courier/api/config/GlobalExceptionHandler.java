package com.courier.api.config;

import com.courier.common.exception.NotificationNotFoundException;
import com.courier.common.exception.RateLimitExceededException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /*
     * Bean Validation failure - @NotBlank, @NotNull, @Size violations.
     * Spring throws this automatically when @Valid fails.
     * Returns HTTP 400 with a map of field → error message.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(
            MethodArgumentNotValidException ex) {

        Map<String, String> fieldErrors = new HashMap<>();
        for (FieldError error : ex.getBindingResult().getFieldErrors()) {
            fieldErrors.put(error.getField(), error.getDefaultMessage());
        }

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(errorBody("VALIDATION_FAILED", fieldErrors.toString()));
    }

    /*
     * Rate limit exceeded.
     * Returns HTTP 429 Too Many Requests with Retry-After header.
     * Retry-After tells the client how many seconds to wait before retrying.
     * This is part of the HTTP spec for rate limiting responses.
     */
    @ExceptionHandler(RateLimitExceededException.class)
    public ResponseEntity<Map<String, Object>> handleRateLimit(
            RateLimitExceededException ex) {

        log.warn("Rate limit exceeded: {}", ex.getMessage());

        return ResponseEntity
                .status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", "60")
                .body(errorBody("RATE_LIMIT_EXCEEDED", ex.getMessage()));
    }

    /*
     * Notification not found.
     * Returns HTTP 404 with no body - no point describing
     * what doesn't exist.
     */
    @ExceptionHandler(NotificationNotFoundException.class)
    public ResponseEntity<Map<String,Object>> handleNotFound(
            NotificationNotFoundException ex) {

        log.warn("Notification not found: {}", ex.getMessage());
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(errorBody("NOT_FOUND", ex.getMessage()));
    }

    /*
     * Catch-all for any unhandled exception.
     * Returns HTTP 500 with a GENERIC message - never expose
     * internal error details to clients. Log the full exception
     * internally for debugging.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneric(Exception ex) {
        log.error("Unhandled exception", ex);

        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(errorBody("INTERNAL_ERROR",
                        "An unexpected error occurred. Please try again later."));
    }

    private Map<String, Object> errorBody(String code, String message) {
        Map<String, Object> body = new HashMap<>();
        body.put("error", code);
        body.put("message", message);
        body.put("timestamp", Instant.now().toString());
        return body;
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalState(
            IllegalStateException ex) {
        log.warn("Invalid operation: {}", ex.getMessage());
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(errorBody("INVALID_OPERATION", ex.getMessage()));
    }
}