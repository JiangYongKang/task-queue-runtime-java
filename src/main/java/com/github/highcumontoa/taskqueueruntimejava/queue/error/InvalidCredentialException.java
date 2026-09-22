package com.github.highcumontoa.taskqueueruntimejava.queue.error;

public class InvalidCredentialException extends QueueRuntimeException {
    public InvalidCredentialException(ErrorCode errorCode, String message) { super(errorCode, message); }
    public InvalidCredentialException(ErrorCode errorCode, String message, Throwable cause) { super(errorCode, message, cause); }
}
