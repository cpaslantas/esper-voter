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
 
    private static PhoneCallGenerator generator;
    private static EsperDataConnector dc;
    private static long startTime = 0;
 
    public static void GenerateVote(EPRuntime cepRT) {
 
        PhoneCall pc = generator.receive();
        cepRT.sendEvent(pc);
 
    }
    
    public static void startThreads(int numberOfThreads,
            int numberOfTicksToSend,
            int numberOfSecondsWaitForCompletion,
            EPServiceProvider epService,
            int inputRate)
	{
		final int totalNumTicks = numberOfTicksToSend;
		
		ThreadPoolExecutor pool = new ThreadPoolExecutor(0, numberOfThreads, 99999, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		
		System.out.println(".performTest Starting thread pool, threads=" + numberOfThreads);
		pool.setCorePoolSize(numberOfThreads);
		
		startTime = System.nanoTime();
		VoteSender runnable = new VoteSender(epService, generator, dc);
		long sleepTime = 0l;
		if(inputRate > 0)
			sleepTime = 1000l/((long)inputRate);
		
		for (int i = 0; i < totalNumTicks; i++)
		{
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			pool.execute(runnable);
		}
		
		System.out.println(".performTest Listening for completion");
		EPRuntimeUtil.awaitCompletion(epService.getEPRuntime(), totalNumTicks, numberOfSecondsWaitForCompletion, 1, 10);
		
		pool.shutdown();
	}
    
 
    public static void main(String[] args) {
    	int numThreads = 1;
    	
    	//process the arguments
    	for(int i = 0; i < args.length; i++){
    		String arg[] = args[i].split("=");
    		if(arg.length <= 1) {
    			System.out.println("WARNING: arg " + args[i] + " is not valid");
    			continue;
    		}
    		String param = arg[0];
    		String value = arg[1];
    		if(param.equals("-threads") || param.equals("-t")) {
    			numThreads = new Integer(value);
    		}
    		else if(param.equals("-votefile") || param.equals("-vf")) {
    			VoterConstants.VOTE_FILE = value;
    		}
    		else if(param.equals("-votedir") || param.equals("-dir")) {
    			VoterConstants.VOTE_DIR = value;
    		}
    		else if(param.equals("-inputrate") || param.equals("-ir")) {
    			VoterConstants.INPUT_RATE = new Integer(value);
    		}
    	}

    	String vf = VoterConstants.VOTE_DIR + VoterConstants.VOTE_FILE;
    	System.out.println(vf);
    	System.out.println("Num Threads: " + numThreads);
    	generator = new PhoneCallGenerator(vf);
    	
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
 
       startThreads(numThreads, 10000, 30, cep, VoterConstants.INPUT_RATE);
       System.out.println("Total Time: " + (System.nanoTime() - startTime)/1000000l);
       System.out.println(dc.printStats());
        
    } 
}