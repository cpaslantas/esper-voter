package edu.brown.benchmark.voteresper.tuples;


public class EndWorkflow extends EsperTuple{
    public final long timestamp;
    public final int lastSP;
	
    public EndWorkflow(int lastSP, long timestamp, long originTS) {
    	super("EndWorkflow");
    	this.lastSP = lastSP;
        this.timestamp = timestamp;
        this.tupleStartTime = originTS;
    }
    
    public int getLastSP() {return lastSP;}
    public long getTimestamp() {return timestamp;}
    public long getStartTime() {return startTime;}
    public long getEndTime() {return endTime;}
    public long getTupleStartTime() {return tupleStartTime;}
    
    public String toString() {
    	return "End Workflow";
    }

}