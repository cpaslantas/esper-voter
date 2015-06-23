package edu.brown.benchmark.voteresper.tuples;

import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voteresper.VoterConstants;


public class Vote extends EsperTuple{
    public final long voteId;
    public final int contestantNumber;
    public final long phoneNumber;
    public final String state;
    public final long timestamp;
	
    public Vote(long voteId, int contestantNumber, long phoneNumber, String state, long timestamp, long originTS) {
    	super("Vote");
        this.voteId = voteId;
        this.contestantNumber = contestantNumber;
        this.phoneNumber = phoneNumber;
        this.state = state;
        this.timestamp = timestamp;
        this.tupleStartTime = originTS;
    }
    
    public Vote(PhoneCall pc, String state, long timestamp) {
    	super("Vote");
        this.voteId = pc.voteId;
        this.contestantNumber = pc.contestantNumber;
        this.phoneNumber = pc.phoneNumber;
        this.state = state;
        this.timestamp = timestamp;
        this.tupleStartTime = pc.tupleStartTime;
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
    
    public static String outputColumns() {
    	return new String("vote_id,contestant_number,phone_number,state,created");
    }
    
    public String outputValues() {
    	return new String(voteId + "," + contestantNumber + "," + phoneNumber + ",'" + state + "'," + timestamp);
    }
    
    public String toParams() {
    	return "" + voteId + "," + phoneNumber + "," + state + "," + contestantNumber + "," + timestamp;
    }
}