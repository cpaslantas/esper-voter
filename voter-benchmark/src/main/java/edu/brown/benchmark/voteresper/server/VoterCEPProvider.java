package edu.brown.benchmark.voteresper.server;

import com.espertech.esper.client.*;

import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.dataconnectors.DummyDataConnector;
import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;
import edu.brown.benchmark.voteresper.dataconnectors.EsperTableConnector;
import edu.brown.benchmark.voteresper.dataconnectors.VoltDBAdHocConnector;
import edu.brown.benchmark.voteresper.dataconnectors.VoltDBSPConnector;
import edu.brown.benchmark.voteresper.listeners.*;
import edu.brown.benchmark.voteresper.server.CEPProvider.ICEPProvider;
import edu.brown.benchmark.voteresper.tuples.*;

public class VoterCEPProvider implements ICEPProvider {

    private EPAdministrator cepAdm;

    private EPRuntime epRuntime;
    private EsperDataConnector dc;
    private StatsCollector stats;

    public VoterCEPProvider() {
    }

    public void init(final int _sleepListenerMillis, boolean order, String backend) {
        Configuration cepConfig;

        // EsperHA enablement - if available
        try {
            Class configurationHAClass = Class.forName("com.espertech.esperha.client.ConfigurationHA");
            cepConfig = (Configuration) configurationHAClass.newInstance();
            System.out.println("=== EsperHA is available, using ConfigurationHA ===");
        } catch (ClassNotFoundException e) {
        	cepConfig = new Configuration();
        } catch (Throwable t) {
            System.err.println("Could not properly determine if EsperHA is available, default to Esper");
            t.printStackTrace();
            cepConfig = new Configuration();
        }
        cepConfig.addEventType("PhoneCall", PhoneCall.class.getName());
        cepConfig.addEventType("Vote", Vote.class.getName());
        cepConfig.addEventType("ToDelete", ToDelete.class.getName());


        // EsperJMX enablement - if available
		try {
			Class.forName("com.espertech.esper.jmx.client.EsperJMXPlugin");
			cepConfig.addPluginLoader(
                    "EsperJMX",
                    "com.espertech.esper.jmx.client.EsperJMXPlugin",
    				null);// will use platform mbean - should enable platform mbean connector in startup command line
            System.out.println("=== EsperJMX is available, using platform mbean ===");
		} catch (ClassNotFoundException e) {
			;
		}
		
		//REMOVES ORDER
		if(!order) {
			cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false); //removes order-preserving
			cepConfig.getEngineDefaults().getThreading().setInsertIntoDispatchPreserveOrder(false);
			cepConfig.getEngineDefaults().getThreading()
	          .setListenerDispatchPreserveOrder(false);
			cepConfig.getEngineDefaults().getThreading()
	          .setInternalTimerEnabled(false);   // remove thread that handles time advancing
		}
		//END REMOVE ORDER


        EPServiceProvider epService = EPServiceProviderManager.getProvider("VoterDemo", cepConfig);
        stats = new StatsCollector();
        System.out.println("XXXXX " + backend + " XXXXXX");
        
        if(backend.equalsIgnoreCase(VoterConstants.ESPER_BACKEND))
        	dc = new EsperTableConnector(VoterConstants.NUM_CONTESTANTS, epService, stats);
        else if (backend.equalsIgnoreCase(VoterConstants.VOLTDBADHOC_BACKEND))
        	dc = new VoltDBAdHocConnector(VoterConstants.NUM_CONTESTANTS, epService, stats);
        else if (backend.equalsIgnoreCase(VoterConstants.VOLTDB_BACKEND))
        	dc = new VoltDBSPConnector(VoterConstants.NUM_CONTESTANTS, epService, stats);
        else if (backend.equalsIgnoreCase(VoterConstants.DUMMY_BACKEND))
        	dc = new DummyDataConnector(VoterConstants.NUM_CONTESTANTS, epService, stats);
        else
        	dc = new VoltDBSPConnector(VoterConstants.NUM_CONTESTANTS, epService, stats);
        
        cepAdm = epService.getEPAdministrator();
        
        EPStatement phoneCallStatement = cepAdm.createEPL("select * from " +
                "PhoneCall(contestantNumber>0)");
        EPStatement voteWindowStmt;
        EPStatement voteDeleteStmt;
        EPStatement voteStmt;
        if(backend.equalsIgnoreCase(VoterConstants.VOLTDB_BACKEND)) {
        	voteWindowStmt = cepAdm.createEPL("select * from " +
        			"Vote");
        	voteDeleteStmt = cepAdm.createEPL("select * from " +
        			"ToDelete");
        	phoneCallStatement.addListener(new PhoneCallListener(epService, dc));
            voteWindowStmt.addListener(new VoteWindowListener(epService, dc));
            voteDeleteStmt.addListener(new VoteDeleteListener(epService, dc));
        } else {
        	System.out.println("Window state in Esper");
        	voteWindowStmt = cepAdm.createEPL("select * from " +
        			"Vote.win:length_batch(" + VoterConstants.WIN_SLIDE + ")");
        	voteDeleteStmt = cepAdm.createEPL("select * from " +
        			"ToDelete");
//                "Vote.win:length_batch(" + VoterConstants.VOTE_THRESHOLD + ")");
          voteStmt = cepAdm.createEPL("select * from " +
        		 "Vote");
          phoneCallStatement.addListener(new PhoneCallListener(epService, dc));
          voteWindowStmt.addListener(new VoteWindowListener(epService, dc));
          voteDeleteStmt.addListener(new VoteDeleteListener(epService, dc));
          voteStmt.addListener(new WorkflowEndListener(epService, dc));
        }
    
        
        //subscriber = new MySubscriber();
        epRuntime = epService.getEPRuntime();
    }

    public void registerStatement(String statement, String statementID) {
        EPStatement stmt = cepAdm.createEPL(statement, statementID);
    }

    public void sendEvent(Object theEvent) {
        epRuntime.sendEvent(theEvent);
    }
    
    public StatsCollector getStatsCollector() {
    	return stats;
    }
}
