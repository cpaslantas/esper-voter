package edu.brown.benchmark.voteresper.listeners;

import java.util.concurrent.atomic.AtomicInteger;

import com.espertech.esper.client.*;

import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;
import edu.brown.benchmark.voteresper.tuples.PhoneCall;
import edu.brown.benchmark.voteresper.tuples.Vote;

public class PhoneCallListener implements UpdateListener {
	
	EsperDataConnector dc;
	EPServiceProvider epService;
	
	public PhoneCallListener(EPServiceProvider epService, EsperDataConnector dc){
		this.dc = dc;
		this.epService = epService;
	}
		 
    public void update(EventBean[] newData, EventBean[] oldData) {
    	if(!dc.stats.isStarted())
    		dc.stats.start();
    	
    	PhoneCall pc = (PhoneCall) newData[0].getUnderlying();
        boolean exists = dc.realContestant(pc.contestantNumber);
        long numVotes = dc.numTimesVoted(pc.phoneNumber);
        String state = dc.getState(pc.phoneNumber);
        
        if(!exists){
            dc.stats.addStat(VoterConstants.VOTE_KEY, pc);
            dc.closeWorkflow(pc);
        	return;
        }

        if(numVotes >= VoterConstants.MAX_VOTES){
            dc.stats.addStat(VoterConstants.VOTE_KEY, pc);
            dc.closeWorkflow(pc);
        	return;
        }
        
        Vote v = new Vote(pc, state, System.nanoTime());
        dc.insertVote(v);
        
        dc.stats.addStat(VoterConstants.VOTE_KEY, pc);
        v.startTime = System.nanoTime();
        
        EPRuntime cepRT = epService.getEPRuntime();
        cepRT.sendEvent(v);
    }
}
