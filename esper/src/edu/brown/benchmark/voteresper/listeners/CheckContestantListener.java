package edu.brown.benchmark.voteresper.listeners;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import edu.brown.benchmark.voteresper.PhoneCall;
import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;

public class CheckContestantListener implements UpdateListener {
	
	EsperDataConnector dc;
	
	public CheckContestantListener(EsperDataConnector dc){
		this.dc = dc;
	}
		 
    public void update(EventBean[] newData, EventBean[] oldData) {
    	System.out.println("CheckContestantListener");
        PhoneCall tick = (PhoneCall) newData[0].get("tick");
        System.out.println("1");
        boolean exists = dc.realContestant(tick.contestantNumber);

        if(exists)
        	System.out.println("Contestant " + tick.contestantNumber + " exists");
        else
        	System.out.println("Contestant " + tick.contestantNumber + " does not exist");
    }
}
