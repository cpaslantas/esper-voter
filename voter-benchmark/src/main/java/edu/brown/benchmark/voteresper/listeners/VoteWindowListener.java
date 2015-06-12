package edu.brown.benchmark.voteresper.listeners;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;
import edu.brown.benchmark.voteresper.tuples.PhoneCall;
import edu.brown.benchmark.voteresper.tuples.Vote;

public class VoteWindowListener implements UpdateListener {
	EsperDataConnector dc;
	EPServiceProvider epService;
	int numRuns = 0;
	
	public VoteWindowListener(EPServiceProvider epService, EsperDataConnector dc){
		this.dc = dc;
		this.epService = epService;
	}
		 
    public void update(EventBean[] newData, EventBean[] oldData) {
//    	numRuns++;
//    	if(numRuns % 1000 == 0)
//        	System.out.println("VOTE WINDOW LISTENER: " + numRuns);
    	
    	long startTime = System.nanoTime();
    	
    	Vote v = (Vote) newData[0].getUnderlying();
    	
    	if(newData.length < VoterConstants.WIN_SLIDE){
    		System.out.println("ERROR: FEWER THAN " + VoterConstants.WIN_SLIDE + " ROWS IN WINDOW SLIDE");
    		dc.stats.addStat(VoterConstants.LEADERBOARD_KEY, v);
    	}
    	
    	int winSize = (int)dc.getLeaderboardSize();
    	long cutoffVote = 0;
    	if(winSize >= VoterConstants.WIN_SIZE) {
    		cutoffVote = dc.getCutoffVote();
    		dc.deleteCutoff(cutoffVote);
    	}
    	
    	for(int i = 0; i < VoterConstants.WIN_SLIDE; i++) {    
    		v = (Vote)newData[i].getUnderlying();
    		if(cutoffVote < v.voteId)
    			cutoffVote = v.voteId;
    		dc.insertLeaderboard(v);
    	}
    	dc.setCutoffVote(cutoffVote);
    	dc.stats.addStat(VoterConstants.LEADERBOARD_KEY, v);
    	
    }
}
