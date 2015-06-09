package edu.brown.benchmark.voteresper.listeners;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;
import edu.brown.benchmark.voteresper.tuples.Vote;

public class WorkflowEndListener implements UpdateListener {
	EsperDataConnector dc;
	EPServiceProvider epService;
	
	public WorkflowEndListener(EPServiceProvider epService, EsperDataConnector dc){
		this.dc = dc;
		this.epService = epService;
	}
		 
    public void update(EventBean[] newData, EventBean[] oldData) {

    	Vote v = (Vote) newData[0].getUnderlying();
    	
    	dc.closeWorkflow(v);
    	
    }
}
