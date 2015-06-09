package edu.brown.benchmark.voteresper.server;

import com.espertech.esper.client.*;

import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;
import edu.brown.benchmark.voteresper.listeners.*;
import edu.brown.benchmark.voteresper.server.CEPProvider.ICEPProvider;
import edu.brown.benchmark.voteresper.tuples.*;

public class VoterCEPProvider implements ICEPProvider {

    private EPAdministrator cepAdm;

    private EPRuntime epRuntime;

    // only one of those 2 will be attached to statement depending on the -mode selected
    private UpdateListener updateListener;

    private static int sleepListenerMillis;
    private static EsperDataConnector dc;

    public VoterCEPProvider() {
    }

    public void init(final int _sleepListenerMillis, boolean order) {
        sleepListenerMillis = _sleepListenerMillis;
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
        //dc = new EsperTableConnector(VoterConstants.NUM_CONTESTANTS);
        cepAdm = epService.getEPAdministrator();
        
        EPStatement phoneCallStatement = cepAdm.createEPL("select * from " +
                "PhoneCall(contestantNumber>0)");
        EPStatement voteWindowStmt = cepAdm.createEPL("select * from " +
                "Vote.win:length_batch(" + VoterConstants.WIN_SLIDE + ")");
        EPStatement voteDeleteStmt = cepAdm.createEPL("select * from " +
                "Vote.win:length_batch(" + VoterConstants.VOTE_THRESHOLD + ")");
        EPStatement voteStmt = cepAdm.createEPL("select * from " +
                "Vote");
        
        phoneCallStatement.addListener(new PhoneCallListener(epService, dc));
        voteWindowStmt.addListener(new VoteWindowListener(epService, dc));
        voteDeleteStmt.addListener(new VoteDeleteListener(epService, dc));
        voteStmt.addListener(new WorkflowEndListener(epService, dc));
        
        
        //subscriber = new MySubscriber();
        epRuntime = epService.getEPRuntime();
    }

    public void registerStatement(String statement, String statementID) {
        EPStatement stmt = cepAdm.createEPL(statement, statementID);
    }

    public void sendEvent(Object theEvent) {
        epRuntime.sendEvent(theEvent);
    }
}
