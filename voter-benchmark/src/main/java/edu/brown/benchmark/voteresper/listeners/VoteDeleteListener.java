package edu.brown.benchmark.voteresper.listeners;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;
import edu.brown.benchmark.voteresper.tuples.Vote;

public class VoteDeleteListener implements UpdateListener {
	EsperDataConnector dc;
	EPServiceProvider epService;
	
	public VoteDeleteListener(EPServiceProvider epService, EsperDataConnector dc){
		this.dc = dc;
		this.epService = epService;
	}
		 
    public void update(EventBean[] newData, EventBean[] oldData) {
    	//System.out.println("VoteDeleteListener - " + dc.getAllVotesEver());
    	
    	int lowest = dc.findLowestContestant();
    	long numContestants = dc.getNumRemainingContestants();
    	
    	if(numContestants <= 1) {
    		System.out.println("Not enough contestants to remove");
    		return;
    	}
    	
    	dc.removeVotes(lowest);
    	dc.removeContestant(lowest);
    	
    	if(newData.length < 1)
    		return;
    	Vote v = (Vote)newData[0].getUnderlying();
    	dc.stats.addStat(VoterConstants.DELETE_KEY, v);
    }
}
