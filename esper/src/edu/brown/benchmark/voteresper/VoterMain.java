package edu.brown.benchmark.voteresper;

import com.espertech.esper.client.*;
import java.util.Random;
import java.util.Date;
 
public class VoterMain {
 
    private static PhoneCallGenerator generator = new PhoneCallGenerator();
    
    
 
    public static void GenerateVote(EPRuntime cepRT) {
 
        PhoneCall pc = generator.receive();
        System.out.println("Sending call:" + pc);
        cepRT.sendEvent(pc);
 
    }
 
    public static class CEPListener implements UpdateListener {
 
        public void update(EventBean[] newData, EventBean[] oldData) {
            System.out.println("Event received: " + newData[0].getUnderlying());
        }
    }
    
    
 
    public static void main(String[] args) {
 
    	//The Configuration is meant only as an initialization-time object.
        Configuration cepConfig = new Configuration();
        cepConfig.addEventType("VoteTick", PhoneCall.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
        EPRuntime cepRT = cep.getEPRuntime();
 
        EPAdministrator cepAdm = cep.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL("select * from " +
                "VoteTick(contestantNumber=1).win:length(30)");
 
        cepStatement.addListener(new CEPListener());
        System.out.println("VOTER MAIN");
 
       // We generate a few ticks...
        for (int i = 0; i < 5; i++) {
            GenerateVote(cepRT);
        }
    }
}