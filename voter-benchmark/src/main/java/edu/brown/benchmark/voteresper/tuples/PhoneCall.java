package edu.brown.benchmark.voteresper.tuples;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import edu.brown.benchmark.voteresper.MarketData;
import edu.brown.benchmark.voteresper.Symbols;
import edu.brown.benchmark.voteresper.VoterConstants;

public class PhoneCall extends EsperTuple {
	public final static int SIZE = Long.SIZE + Integer.SIZE + Long.SIZE + Long.SIZE;
    static {
        System.out.println("PhoneCall event = " + SIZE + " bit = " + SIZE/8 + " bytes");
        System.out.println("  100 Mbit/s <==> " + (int) (100*1024*1024/SIZE/1000) + "k evt/s");
        System.out.println("    1 Gbit/s <==> " + (int) (1024*1024*1024/SIZE/1000) + "k evt/s");
    }
	
    public final long voteId;
    public final int contestantNumber;
    public final long phoneNumber;
    private long time;//ms
    private final long inTime;
	
    public PhoneCall(long voteId, int contestantNumber, long phoneNumber) {
    	super("PhoneCall");
        this.voteId = voteId;
        this.contestantNumber = contestantNumber;
        this.phoneNumber = phoneNumber;
        this.inTime = System.nanoTime();
        this.tupleStartTime = startTime;
    }
    
    public PhoneCall(String s) {
    	super("PhoneCall");
    	String[] split = s.split(" ");
    	
        this.voteId = new Long(split[0]);
        this.phoneNumber = new Long(split[1]);
        this.contestantNumber = new Integer(split[2]);
        this.inTime = System.nanoTime();
        this.tupleStartTime = startTime;
    }
    
    public long getVoteId() {return voteId;}
    public int getContestantNumber() {return contestantNumber;}
    public long getPhoneNumber() {return phoneNumber;}
    public long getTime() {return time;}
    public long getInTime() {return inTime;}
    
    public void setTime(long time) { this.time = time; }
    
    public void toByteBuffer(ByteBuffer b) {
        b.putLong(voteId);//we know ticker is a fixed length string
        b.putInt(contestantNumber);
        b.putLong(phoneNumber);
        b.putLong(System.currentTimeMillis());
    }

    public static PhoneCall fromByteBuffer(ByteBuffer byteBuffer) {       
        long voteId = byteBuffer.getLong();
        int contestantNumber = byteBuffer.getInt();
        long phoneNumber = byteBuffer.getLong();
        long time = byteBuffer.getLong();
        
        PhoneCall pc = new PhoneCall(voteId, contestantNumber, phoneNumber);
        pc.setTime(time);
        return pc;
    }
    
    public String toString() {
    	return "VoteID: " + voteId + " ContestantNumber: " + contestantNumber + " PhoneNumber: " + phoneNumber;
    }
    
    public String toParams() {
    	return "" + voteId + "," + phoneNumber + "," + contestantNumber + "," + VoterConstants.NUM_CONTESTANTS;
    }
}