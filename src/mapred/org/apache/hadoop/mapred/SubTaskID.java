package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapreduce.ID;

public class SubTaskID extends org.apache.hadoop.mapreduce.ID{
    protected static final String subtask = "subtask";
    private TaskAttemptID taskid;
    
    public SubTaskID(TaskAttemptID taskid, int subtaskid) {
    	super(subtaskid);
    	this.taskid = taskid;
    }
    
    public TaskAttemptID getTaskAttemptID() {
    	return taskid; 
    }
    
    public TaskID getTaskID() {
    	return taskid.getTaskID();
    }
    
    public JobID getJobID() {
    	return taskid.getJobID();
    }
    
    public boolean equals(Object o) {
    	if (!super.equals(o))
    		return false;
    	SubTaskID that = (SubTaskID)o;
    	return this.taskid.equals(that.taskid);
    }
    
    public int hashCode() {
    	return taskid.hashCode() * 5 + id;
    }
    
    public int compareTo(ID o) {
    	SubTaskID that = (SubTaskID)o;
    	int taskidcmp = this.taskid.compareTo(that.taskid);
    	if (taskidcmp == 0) {
    		return this.id - that.id;
    	}
    	else
    		return taskidcmp;
    }
    
    public String toString() {
    	return taskid.toString()+"_"+id;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      taskid.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      taskid.write(out);
    }
}
