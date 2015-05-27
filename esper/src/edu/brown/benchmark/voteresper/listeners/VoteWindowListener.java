package edu.brown.benchmark.voteresper.listeners;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import edu.brown.benchmark.voteresper.PhoneCall;
import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.Vote;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;

public class VoteWindowListener implements UpdateListener {
	EsperDataConnector dc;
	EPServiceProvider epService;
	StatsCollector stats;
	
	public VoteWindowListener(EPServiceProvider epService, EsperDataConnector dc, StatsCollector stats){
		this.dc = dc;
		this.epService = epService;
		this.stats = stats;
	}
		 
    public void update(EventBean[] newData, EventBean[] oldData) {
    	Vote v = (Vote) newData[0].getUnderlying();
    	v.endTime = System.nanoTime();
    	stats.addStat(VoterConstants.LEADERBOARD_KEY, v.startTime, v.endTime);
    }
}
