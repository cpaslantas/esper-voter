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
        cepRT.sendEvent(pc);
 
    }
    
    
 
    public static void main(String[] args) {
    	dc = new DummyDataConnector(VoterConstants.NUM_CONTESTANTS);
 
    	//The Configuration is meant only as an initialization-time object.
        Configuration cepConfig = new Configuration();
        cepConfig.addEventType("PhoneCall", PhoneCall.class.getName());
        cepConfig.addEventType("Vote", Vote.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
        EPRuntime cepRT = cep.getEPRuntime();
 
        EPAdministrator cepAdm = cep.getEPAdministrator();
        EPStatement phoneCallStatement = cepAdm.createEPL("select * from " +
                "PhoneCall(contestantNumber>0)");
        EPStatement voteWindowStmt = cepAdm.createEPL("select * from " +
                "Vote.win:length_batch(100)");
        EPStatement voteDeleteStmt = cepAdm.createEPL("select * from " +
                "Vote.win:length_batch(1000)");
 
        phoneCallStatement.addListener(new PhoneCallListener(cep, dc));
        voteDeleteStmt.addListener(new VoteDeleteListener(cep, dc));
        System.out.println("VOTER MAIN");
 
       // We generate a few ticks...
        for (int i = 0; i < 10000; i++) {
            GenerateVote(cepRT);
        }
        System.out.println(dc.printStats());
    }
}