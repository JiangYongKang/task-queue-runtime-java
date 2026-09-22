package com.github.highcumontoa.taskqueueruntimejava.queue.error;

public class AuthorizationException extends QueueRuntimeException {
    public AuthorizationException(ErrorCode errorCode, String message) { super(errorCode, message); }
    public AuthorizationException(ErrorCode errorCode, String message, Throwable cause) { super(errorCode, message, cause); }
}
