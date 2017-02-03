package io.jitter.tasks;

public class TaskIsAlreadyRunningException extends Exception {

    public TaskIsAlreadyRunningException(String message) {
        super(message);
    }
}
