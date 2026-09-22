package com.github.highcumontoa.taskqueueruntimejava.queue.error;

/** 所有队列运行时异常的基类，携带稳定错误码与可解释上下文。 */
public class QueueRuntimeException extends RuntimeException {
    private final ErrorCode errorCode;

    public QueueRuntimeException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public QueueRuntimeException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }
}
