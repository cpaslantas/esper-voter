package edu.brown.benchmark.voteresper.listeners;

import com.espertech.esper.client.*;

import edu.brown.benchmark.voteresper.PhoneCall;
import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.Vote;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;

public class PhoneCallListener implements UpdateListener {
	
	EsperDataConnector dc;
	EPServiceProvider epService;
	StatsCollector stats;
	
	public PhoneCallListener(EPServiceProvider epService, EsperDataConnector dc, StatsCollector stats){
		this.dc = dc;
		this.epService = epService;
		this.stats = stats;
	}
		 
    public void update(EventBean[] newData, EventBean[] oldData) {
    	PhoneCall pc = (PhoneCall) newData[0].getUnderlying();
        boolean exists = dc.realContestant(pc.contestantNumber);
        long numVotes = dc.numTimesVoted(pc.phoneNumber);
        String state = dc.getState(pc.phoneNumber);
        
        if(!exists){
        	//System.out.println(pc.contestantNumber + " not valid!");
        	return;
        }

        if(numVotes >= VoterConstants.MAX_VOTES){
        	//System.out.println(pc.phoneNumber + " over the max vote limit!");
        	return;
        }
        
        Vote v = new Vote(pc, state, System.nanoTime(), pc.tupleStartTime);
        dc.insertVote(v);
        pc.endTime = System.nanoTime(); 
        stats.addStat(VoterConstants.VOTE_KEY, pc.startTime, pc.endTime);
        v.startTime = System.nanoTime();
        EPRuntime cepRT = epService.getEPRuntime();
        cepRT.sendEvent(v);
    }
}
