package edu.brown.benchmark.voteresper;

public class Vote {
    public final long voteId;
    public final int contestantNumber;
    public final long phoneNumber;
    public final String state;
    public final long timestamp;
	
    public Vote(long voteId, int contestantNumber, long phoneNumber, String state, long timestamp) {
        this.voteId = voteId;
        this.contestantNumber = contestantNumber;
        this.phoneNumber = phoneNumber;
        this.state = state;
        this.timestamp = timestamp;
    }
    
    public Vote(PhoneCall pc, String state, long timestamp) {
        this.voteId = pc.voteId;
        this.contestantNumber = pc.contestantNumber;
        this.phoneNumber = pc.phoneNumber;
        this.state = state;
        this.timestamp = timestamp;
    }
    
    public long getVoteId() {return voteId;}
    public int getContestantNumber() {return contestantNumber;}
    public long getPhoneNumber() {return phoneNumber;}
    public String getState() {return state;}
    public long getTimestamp() {return timestamp;}
    
    public String toString() {
    	return "VoteID: " + voteId + " ContestantNumber: " + contestantNumber + " PhoneNumber: " + phoneNumber;
    }
}