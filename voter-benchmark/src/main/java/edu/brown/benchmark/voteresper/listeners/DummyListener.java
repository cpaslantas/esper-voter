package edu.brown.benchmark.voteresper.listeners;

import java.util.concurrent.atomic.AtomicInteger;

import com.espertech.esper.client.*;

import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;
import edu.brown.benchmark.voteresper.tuples.PhoneCall;
import edu.brown.benchmark.voteresper.tuples.Vote;

public class DummyListener implements UpdateListener {
	
	EsperDataConnector dc;
	EPServiceProvider epService;
	
	public DummyListener(EPServiceProvider epService, EsperDataConnector dc){
		this.dc = dc;
		this.epService = epService;
	}
		 
    public void update(EventBean[] newData, EventBean[] oldData) {
    	if(!dc.stats.isStarted())
    		dc.stats.start();
    	
    	PhoneCall pc = (PhoneCall) newData[0].getUnderlying();
        
        dc.stats.addStat(VoterConstants.DUMMY_KEY, pc.startTime, System.nanoTime());
    }
}
