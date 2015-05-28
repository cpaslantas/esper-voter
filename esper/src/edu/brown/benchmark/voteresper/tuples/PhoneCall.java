package edu.brown.benchmark.voteresper.tuples;

public class PhoneCall extends EsperTuple{
    public final long voteId;
    public final int contestantNumber;
    public final long phoneNumber;
	
    public PhoneCall(long voteId, int contestantNumber, long phoneNumber) {
    	super("PhoneCall");
        this.voteId = voteId;
        this.contestantNumber = contestantNumber;
        this.phoneNumber = phoneNumber;
        this.tupleStartTime = startTime;
    }
    
    public PhoneCall(String s) {
    	super("PhoneCall");
    	String[] split = s.split(" ");
    	
        this.voteId = new Long(split[0]);
        this.phoneNumber = new Long(split[1]);
        this.contestantNumber = new Integer(split[2]);
        this.tupleStartTime = startTime;
    }
    
    public void updateStartTime(long t) {
    	this.startTime = t;
    	this.tupleStartTime = t;
    }
    
    public long getVoteId() {return voteId;}
    public int getContestantNumber() {return contestantNumber;}
    public long getPhoneNumber() {return phoneNumber;}
    
    public String toString() {
    	return "VoteID: " + voteId + " ContestantNumber: " + contestantNumber + " PhoneNumber: " + phoneNumber;
    }
}