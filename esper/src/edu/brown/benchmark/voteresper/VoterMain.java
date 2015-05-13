package edu.brown.benchmark.voteresper;

import com.espertech.esper.client.*;

import edu.brown.benchmark.voteresper.dataconnectors.*;
import edu.brown.benchmark.voteresper.listeners.*;

import java.util.Random;
import java.util.Date;
 
public class VoterMain {
 
    private static PhoneCallGenerator generator = new PhoneCallGenerator();
    private static EsperDataConnector dc;
 
    public static void GenerateVote(EPRuntime cepRT) {
 
        PhoneCall pc = generator.receive();
        System.out.println("Sending call:" + pc);
        cepRT.sendEvent(pc);
 
    }
    
    
 
    public static void main(String[] args) {
    	dc = new DummyDataConnector(VoterConstants.NUM_CONTESTANTS);
 
    	//The Configuration is meant only as an initialization-time object.
        Configuration cepConfig = new Configuration();
        cepConfig.addEventType("VoteTick", PhoneCall.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
        EPRuntime cepRT = cep.getEPRuntime();
 
        EPAdministrator cepAdm = cep.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL("select * from " +
                "VoteTick(contestantNumber>0)");
 
        cepStatement.addListener(new CheckContestantListener(dc));
        System.out.println("VOTER MAIN");
 
       // We generate a few ticks...
        for (int i = 0; i < 5; i++) {
            GenerateVote(cepRT);
        }
    }
}