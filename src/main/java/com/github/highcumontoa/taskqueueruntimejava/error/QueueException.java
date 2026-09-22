package com.github.highcumontoa.taskqueueruntimejava.error;

/** 队列运行时唯一受检外的业务异常，携带稳定错误码与可读原因。 */
public class QueueException extends RuntimeException {
    private final ErrorCode code;

    public QueueException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public QueueException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }
}
