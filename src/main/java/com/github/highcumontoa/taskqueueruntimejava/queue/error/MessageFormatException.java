package com.github.highcumontoa.taskqueueruntimejava.queue.error;

public class MessageFormatException extends QueueRuntimeException {
    public MessageFormatException(ErrorCode errorCode, String message) { super(errorCode, message); }
    public MessageFormatException(ErrorCode errorCode, String message, Throwable cause) { super(errorCode, message, cause); }
}
