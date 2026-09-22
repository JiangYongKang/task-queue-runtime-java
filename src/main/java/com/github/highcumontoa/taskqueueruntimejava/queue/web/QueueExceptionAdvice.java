package com.github.highcumontoa.taskqueueruntimejava.queue.web;

import com.github.highcumontoa.taskqueueruntimejava.queue.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.QueueRuntimeException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/** 将运行时错误码映射为稳定、可区分的 HTTP 响应。 */
@RestControllerAdvice
public class QueueExceptionAdvice {

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<Map<String, String>> handleMissingHeader(MissingRequestHeaderException ex) {
        if ("X-Queue-Token".equals(ex.getHeaderName())) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(Map.of("errorCode", ErrorCode.INVALID_CREDENTIAL.name(),
                            "message", "missing credential token"));
        }
        return ResponseEntity.badRequest()
                .body(Map.of("errorCode", "BAD_REQUEST", "message", nullToEmpty(ex.getMessage())));
    }

    @ExceptionHandler(QueueRuntimeException.class)
    public ResponseEntity<Map<String, String>> handle(QueueRuntimeException ex) {
        ErrorCode code = ex.errorCode();
        return ResponseEntity.status(statusOf(code))
                .body(Map.of("errorCode", code.name(), "message", ex.getMessage() == null ? "" : ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest()
                .body(Map.of("errorCode", "BAD_REQUEST", "message", nullToEmpty(ex.getMessage())));
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    static HttpStatus statusOf(ErrorCode code) {
        return switch (code) {
            case INVALID_CREDENTIAL -> HttpStatus.UNAUTHORIZED;
            case PERMISSION_DENIED, CROSS_TOPIC_ACCESS_DENIED -> HttpStatus.FORBIDDEN;
            case UNKNOWN_TOPIC, UNKNOWN_GROUP -> HttpStatus.NOT_FOUND;
            case QUEUE_FULL, BATCH_REJECTED -> HttpStatus.TOO_MANY_REQUESTS;
            case PRODUCE_TIMEOUT, RECEIVE_TIMEOUT, VISIBILITY_TIMEOUT -> HttpStatus.GATEWAY_TIMEOUT;
            case OFFSET_ROLLBACK_REJECTED, CROSS_GROUP_COMMIT_REJECTED, ILLEGAL_RESET_TARGET,
                 OFFSET_OUT_OF_RANGE, UNKNOWN_DELIVERY_TOKEN, DEAD_MESSAGE_IMMUTABLE,
                 RETRY_EXHAUSTED, NON_RETRYABLE, NO_MESSAGE_AVAILABLE -> HttpStatus.CONFLICT;
            case UNKNOWN_MESSAGE_FORMAT -> HttpStatus.UNPROCESSABLE_ENTITY;
            case BACKEND_UNAVAILABLE -> HttpStatus.SERVICE_UNAVAILABLE;
            case RUNTIME_CLOSED, SHUTDOWN_IN_PROGRESS -> HttpStatus.INSUFFICIENT_STORAGE;
        };
    }
}
