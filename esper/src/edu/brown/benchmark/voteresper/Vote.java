package edu.brown.benchmark.voteresper;

public class Vote {
    public final long voteId;
    public final int contestantNumber;
    public final long phoneNumber;
    public final String state;
    public final long timestamp;
    public long tupleStartTime;
    public long startTime;
    public long endTime;
	
    public Vote(long voteId, int contestantNumber, long phoneNumber, String state, long timestamp, long tupleStartTime) {
        this.voteId = voteId;
        this.contestantNumber = contestantNumber;
        this.phoneNumber = phoneNumber;
        this.state = state;
        this.timestamp = timestamp;
        this.tupleStartTime = tupleStartTime;
    }
    
    public Vote(PhoneCall pc, String state, long timestamp, long tupleStartTime) {
        this.voteId = pc.voteId;
        this.contestantNumber = pc.contestantNumber;
        this.phoneNumber = pc.phoneNumber;
        this.state = state;
        this.timestamp = timestamp;
        this.tupleStartTime = tupleStartTime;
    }
    
    public long getVoteId() {return voteId;}
    public int getContestantNumber() {return contestantNumber;}
    public long getPhoneNumber() {return phoneNumber;}
    public String getState() {return state;}
    public long getTimestamp() {return timestamp;}
    public long getStartTime() {return startTime;}
    public long getEndTime() {return endTime;}
    public long getTupleStartTime() {return tupleStartTime;}
    
    public String toString() {
    	return "VoteID: " + voteId + " ContestantNumber: " + contestantNumber + " PhoneNumber: " + phoneNumber;
    }
    
    public String outputValues() {
    	return new String(voteId + "," + contestantNumber + "," + phoneNumber + ",'" + state + "'," + timestamp);
    }
}