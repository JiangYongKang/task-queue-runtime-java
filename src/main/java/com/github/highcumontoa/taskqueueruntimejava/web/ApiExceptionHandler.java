package com.github.highcumontoa.taskqueueruntimejava.web;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.Map;

/**
 * 将 {@link QueueException} 映射为稳定的 JSON 错误体：
 * {code, message, status, timestamp}。错误码即稳定契约，
 * 调用方可据 code 区分 QUEUE_FULL / BACKPRESSURE_TIMEOUT / BACKEND_UNAVAILABLE 等。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static HttpStatus statusFor(ErrorCode code) {
        return switch (code) {
            case INVALID_CREDENTIAL -> HttpStatus.UNAUTHORIZED;
            case PERMISSION_DENIED, CROSS_TOPIC_ACCESS -> HttpStatus.FORBIDDEN;
            case TOPIC_NOT_FOUND, GROUP_NOT_FOUND, DELIVERY_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case TOPIC_ALREADY_EXISTS, GROUP_ALREADY_EXISTS, BAD_REQUEST,
                 UNSUPPORTED_FORMAT, DUPLICATE_PRODUCER_KEY -> HttpStatus.BAD_REQUEST;
            case QUEUE_FULL, BACKPRESSURE_TIMEOUT -> HttpStatus.TOO_MANY_REQUESTS;
            case OPERATION_TIMEOUT -> HttpStatus.REQUEST_TIMEOUT;
            case OFFSET_ROLLBACK_REJECTED, CROSS_GROUP_COMMIT_REJECTED, DELIVERY_STALE ->
                    HttpStatus.CONFLICT;
            case BACKEND_UNAVAILABLE, RUNTIME_CLOSED -> HttpStatus.SERVICE_UNAVAILABLE;
        };
    }

    @ExceptionHandler(QueueException.class)
    public ResponseEntity<Map<String, Object>> handle(QueueException ex) {
        HttpStatus status = statusFor(ex.getCode());
        Map<String, Object> body = Map.of(
                "code", ex.getCode().name(),
                "message", ex.getMessage() == null ? "" : ex.getMessage(),
                "status", status.value(),
                "timestamp", Instant.now().toString());
        return ResponseEntity.status(status).body(body);
    }
}
