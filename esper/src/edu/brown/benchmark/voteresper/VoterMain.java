package edu.brown.benchmark.voteresper;

import com.espertech.esper.client.*;

import edu.brown.benchmark.voteresper.dataconnectors.*;
import edu.brown.benchmark.voteresper.listeners.*;

import java.util.LinkedList;
import java.util.Random;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
 
public class VoterMain {
 
    private static PhoneCallGenerator generator = new PhoneCallGenerator();
    private static EsperDataConnector dc;
    private static long startTime = 0;
 
    public static void GenerateVote(EPRuntime cepRT) {
 
        PhoneCall pc = generator.receive();
        cepRT.sendEvent(pc);
 
    }
    
    public static void startThreads(int numberOfThreads,
            int numberOfTicksToSend,
            int numberOfSecondsWaitForCompletion,
            EPServiceProvider epService)
	{
		final int totalNumTicks = numberOfTicksToSend;
		
		ThreadPoolExecutor pool = new ThreadPoolExecutor(0, numberOfThreads, 99999, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		startTime = System.nanoTime();
		for (int i = 0; i < totalNumTicks; i++)
		{
			VoteSender runnable = new VoteSender(epService, generator, dc);
			pool.execute(runnable);
		}
		
		System.out.println(".performTest Starting thread pool, threads=" + numberOfThreads);
		pool.setCorePoolSize(numberOfThreads);
		
		System.out.println(".performTest Listening for completion");
		EPRuntimeUtil.awaitCompletion(epService.getEPRuntime(), totalNumTicks, numberOfSecondsWaitForCompletion, 1, 10);
		
		pool.shutdown();
	}
    
 
    public static void main(String[] args) {
    	//The Configuration is meant only as an initialization-time object.
        Configuration cepConfig = new Configuration();
        //configuration changes
        cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(true); //removes order-preserving
        cepConfig.getEngineDefaults().getThreading().setInsertIntoDispatchPreserveOrder(true);
        //end configuration changes
        
        cepConfig.addEventType("PhoneCall", PhoneCall.class.getName());
        cepConfig.addEventType("Vote", Vote.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider("VoterDemo", cepConfig);
        EPRuntime cepRT = cep.getEPRuntime();
        
        dc = new EsperTableConnector(VoterConstants.NUM_CONTESTANTS, cep);
 
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
 
       startThreads(2, 10000, 30, cep);
       System.out.println("Total Time: " + (System.nanoTime() - startTime)/1000000l);
       System.out.println(dc.printStats());
        
    } 
}