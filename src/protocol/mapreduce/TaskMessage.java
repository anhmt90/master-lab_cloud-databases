package protocol.mapreduce;

import mapreduce.common.ApplicationID;
import mapreduce.common.Task;
import mapreduce.common.TaskType;

import java.io.Serializable;

public class TaskMessage implements Serializable {
    Task task;
    CallbackInfo callback;


    public TaskMessage(Task task, CallbackInfo callback) {
        this.task = task;
        this.callback = callback;
    }

    public Task getTask() {
        return task;
    }

    public CallbackInfo getCallback() {
        return callback;
    }
}
