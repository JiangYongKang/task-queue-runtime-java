package com.github.highcumontoa.taskqueueruntimejava.queue.error;

public class OffsetRejectedException extends QueueRuntimeException {
    public OffsetRejectedException(ErrorCode errorCode, String message) { super(errorCode, message); }
    public OffsetRejectedException(ErrorCode errorCode, String message, Throwable cause) { super(errorCode, message, cause); }
}
