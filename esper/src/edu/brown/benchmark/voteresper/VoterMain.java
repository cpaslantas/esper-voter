package edu.brown.benchmark.voteresper;

import com.espertech.esper.client.*;
import java.util.Random;
import java.util.Date;
 
public class VoterMain {
 
    private static PhoneCallGenerator generator = new PhoneCallGenerator();
 
    public static void GenerateVote(EPRuntime cepRT) {
 
    	
    	
        double price = (double) generator.nextInt(10);
        long timeStamp = System.currentTimeMillis();
        String symbol = "AAPL";
        Tick tick = new Tick(symbol, price, timeStamp);
        System.out.println("Sending tick:" + tick);
        cepRT.sendEvent(tick);
 
    }
 
    public static class CEPListener implements UpdateListener {
 
        public void update(EventBean[] newData, EventBean[] oldData) {
            System.out.println("Event received: " + newData[0].getUnderlying());
        }
    }
 
    public static void main(String[] args) {
 
//The Configuration is meant only as an initialization-time object.
        Configuration cepConfig = new Configuration();
        cepConfig.addEventType("StockTick", Tick.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
        EPRuntime cepRT = cep.getEPRuntime();
 
        EPAdministrator cepAdm = cep.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL("select * from " +
                "StockTick(symbol='AAPL').win:length(2) " +
                "having avg(price) > 6.0");
 
        cepStatement.addListener(new CEPListener());
        System.out.println("VOTER MAIN");
 
       // We generate a few ticks...
        for (int i = 0; i < 5; i++) {
            GenerateRandomTick(cepRT);
        }
    }
}