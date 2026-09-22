package com.github.highcumontoa.taskqueueruntimejava.queue.error;

public class QueueFullException extends QueueRuntimeException {
    public QueueFullException(ErrorCode errorCode, String message) { super(errorCode, message); }
    public QueueFullException(ErrorCode errorCode, String message, Throwable cause) { super(errorCode, message, cause); }
}
