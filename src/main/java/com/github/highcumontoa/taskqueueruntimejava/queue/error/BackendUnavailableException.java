package com.github.highcumontoa.taskqueueruntimejava.queue.error;

public class BackendUnavailableException extends QueueRuntimeException {
    public BackendUnavailableException(ErrorCode errorCode, String message) { super(errorCode, message); }
    public BackendUnavailableException(ErrorCode errorCode, String message, Throwable cause) { super(errorCode, message, cause); }
}
