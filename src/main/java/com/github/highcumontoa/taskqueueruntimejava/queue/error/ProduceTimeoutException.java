package com.github.highcumontoa.taskqueueruntimejava.queue.error;

public class ProduceTimeoutException extends QueueRuntimeException {
    public ProduceTimeoutException(ErrorCode errorCode, String message) { super(errorCode, message); }
    public ProduceTimeoutException(ErrorCode errorCode, String message, Throwable cause) { super(errorCode, message, cause); }
}
