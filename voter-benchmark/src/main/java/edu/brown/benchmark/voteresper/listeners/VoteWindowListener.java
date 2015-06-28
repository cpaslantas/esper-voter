package edu.brown.benchmark.voteresper.listeners;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;
import edu.brown.benchmark.voteresper.dataconnectors.VoltDBSPConnector;
import edu.brown.benchmark.voteresper.tuples.PhoneCall;
import edu.brown.benchmark.voteresper.tuples.ToDelete;
import edu.brown.benchmark.voteresper.tuples.Vote;

public class VoteWindowListener implements UpdateListener {
	public static transient Logger LOG = Logger.getLogger(VoterConstants.COMMAND_LOG);
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
    	LOG.debug("exec VoteWindowListener\t" + v.toParams());
    	
    	ToDelete td = null;
    	
//    	if(newData.length < VoterConstants.WIN_SLIDE){
//    		System.out.println("ERROR: FEWER THAN " + VoterConstants.WIN_SLIDE + " ROWS IN WINDOW SLIDE");
//    		dc.stats.addStat(VoterConstants.LEADERBOARD_KEY, v);
//    		return;
//    	}
    	
    	if(dc instanceof VoltDBSPConnector) {
    		td = ((VoltDBSPConnector) dc).runSP2(v);
    	}
    	else {
	    	int winSize = (int)dc.getLeaderboardSize();
	    	long cutoffVote = 0;
	    	if(winSize >= VoterConstants.WIN_SIZE) {
	    		cutoffVote = dc.getCutoffVote();
	    		dc.deleteCutoff(cutoffVote);
	    	}
	    	
	    	assert(newData.length >= VoterConstants.WIN_SLIDE);
	    	
	    	for(int i = 0; i < VoterConstants.WIN_SLIDE; i++) {
	    		v = (Vote) newData[i].getUnderlying();
	    		dc.insertLeaderboard(v);
	    	}
	    	dc.setCutoffVote(cutoffVote + VoterConstants.WIN_SLIDE);
	    	if(dc.getAllVotesEver() % VoterConstants.VOTE_THRESHOLD == 0) {
	    		td = new ToDelete(dc.findLowestContestant(), System.nanoTime(), v.tupleStartTime);
	    	}
    	}
    	
    	dc.stats.addStat(VoterConstants.LEADERBOARD_KEY, v);
    	if(td != null){
    		EPRuntime cepRT = epService.getEPRuntime();
    		cepRT.sendEvent(td);
    	}
    	else {
    		dc.closeWorkflow(v);
    	}
    }
}
