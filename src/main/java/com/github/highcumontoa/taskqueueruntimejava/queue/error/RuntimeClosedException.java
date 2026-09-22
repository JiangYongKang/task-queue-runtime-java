package com.github.highcumontoa.taskqueueruntimejava.queue.error;

public class RuntimeClosedException extends QueueRuntimeException {
    public RuntimeClosedException(ErrorCode errorCode, String message) { super(errorCode, message); }
    public RuntimeClosedException(ErrorCode errorCode, String message, Throwable cause) { super(errorCode, message, cause); }
}
