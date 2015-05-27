package edu.brown.benchmark.voteresper;

public class PhoneCall {
    public final long voteId;
    public final int contestantNumber;
    public final long phoneNumber;
    public long startTime;
    public long endTime;
    public long tupleStartTime;
	
    public PhoneCall(long voteId, int contestantNumber, long phoneNumber) {
        this.voteId = voteId;
        this.contestantNumber = contestantNumber;
        this.phoneNumber = phoneNumber;
    }
    
    public PhoneCall(String s) {
    	String[] split = s.split(" ");
    	
        this.voteId = new Long(split[0]);
        this.phoneNumber = new Long(split[1]);
        this.contestantNumber = new Integer(split[2]);
    }
    
    public long getVoteId() {return voteId;}
    public int getContestantNumber() {return contestantNumber;}
    public long getPhoneNumber() {return phoneNumber;}
    
    public String toString() {
    	return "VoteID: " + voteId + " ContestantNumber: " + contestantNumber + " PhoneNumber: " + phoneNumber;
    }
}