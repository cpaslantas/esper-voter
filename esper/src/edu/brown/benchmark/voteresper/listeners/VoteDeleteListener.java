package edu.brown.benchmark.voteresper.listeners;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.Vote;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;

public class VoteDeleteListener implements UpdateListener {
	EsperDataConnector dc;
	EPServiceProvider epService;
	StatsCollector stats;
	
	public VoteDeleteListener(EPServiceProvider epService, EsperDataConnector dc, StatsCollector stats){
		this.dc = dc;
		this.epService = epService;
		this.stats = stats;
	}
		 
    public void update(EventBean[] newData, EventBean[] oldData) {
    	System.out.println("VoteDeleteListener - " + dc.getAllVotesEver());
    	
    	int lowest = dc.findLowestContestant();
    	long numContestants = dc.getNumRemainingContestants();
    	
    	if(numContestants <= 1) {
    		System.out.println("Not enough contestants to remove");
    		return;
    	}
    	
    	dc.removeContestant(lowest);
    }
}
