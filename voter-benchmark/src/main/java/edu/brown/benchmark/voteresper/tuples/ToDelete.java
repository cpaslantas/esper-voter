package edu.brown.benchmark.voteresper.tuples;


public class ToDelete extends EsperTuple{
    public final int contestantNumber;
    public final long timestamp;
	
    public ToDelete(int contestantNumber, long timestamp, long originTS) {
    	super("ToDelete");
        this.contestantNumber = contestantNumber;
        this.timestamp = timestamp;
        this.tupleStartTime = originTS;
    }
    
    public int getContestantNumber() {return contestantNumber;}
    public long getTimestamp() {return timestamp;}
    public long getStartTime() {return startTime;}
    public long getEndTime() {return endTime;}
    public long getTupleStartTime() {return tupleStartTime;}
    
    public String toString() {
    	return "ContestantNumber: " + contestantNumber;
    }
    
    public String toParams() {
    	return "" + contestantNumber;
    }

}