package edu.brown.benchmark.voteresper;

import com.espertech.esper.client.*;

import edu.brown.benchmark.voteresper.dataconnectors.*;
import edu.brown.benchmark.voteresper.listeners.*;

import java.io.IOException;
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
		double ticksPerMS = -1.0;
		long numberOfNanoSeconds = (long)numberOfSecondsWaitForCompletion * 1000000000;
		
		ThreadPoolExecutor pool = new ThreadPoolExecutor(0, numberOfThreads, 99999, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		
		System.out.println(".performTest Starting thread pool, threads=" + numberOfThreads);
		pool.setCorePoolSize(numberOfThreads);
		
		startTime = System.nanoTime();
		VoteSender vs = new VoteSender(epService, generator, dc);
		if(inputRate > 0)
			ticksPerMS = (double)inputRate/1000.0;
		
		
		int ticksSent = 0;
		long startTime = System.nanoTime();
		long curTime = startTime;
		long curDuration = 0l;
		while(vs.hasVotes() && ticksSent < totalNumTicks)
		{
			if(System.nanoTime() - startTime > numberOfNanoSeconds)
				break;
			curDuration = System.nanoTime() - curTime;
			curTime = System.nanoTime();
			int timesToExecute = (int)((ticksPerMS * curDuration)/1000000.0);
			for(int i = 0; i < timesToExecute; i++) {
				if(!vs.hasVotes() || ticksSent >= totalNumTicks)
					break;
				pool.execute(vs);
				ticksSent++;
				
				try {Thread.sleep(VoterConstants.SLEEP_TIME);} 
				catch (InterruptedException e) {	e.printStackTrace();}
			}
		}
		
		System.out.println(".performTest Listening for completion");
		EPRuntimeUtil.awaitCompletion(epService.getEPRuntime(), totalNumTicks, numberOfSecondsWaitForCompletion, 1, 10);
		
		pool.shutdown();
	}
    
 
    public static void main(String[] args) {
    	int numThreads = 1;
    	int numLines = -1;
    	int duration = 30;
    	
    	//process the arguments
    	for(int i = 0; i < args.length; i++){
    		String arg[] = args[i].split("=");
    		if(arg.length <= 1) {
    			System.out.println("WARNING: arg " + args[i] + " is not valid");
    			continue;
    		}
    		String param = arg[0];
    		String value = arg[1];
    		if(param.equals("-clientthreads") || param.equals("-ct")) {
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
    		else if(param.equals("-votestosend") || param.equals("-vts")) {
    			numLines = new Integer(value);
    		}
    		else if(param.equals("-duration") || param.equals("-d")) {
    			duration = new Integer(value);
    		}
    	}

    	String vf = VoterConstants.VOTE_DIR + VoterConstants.VOTE_FILE;
    	System.out.println(vf);
    	System.out.println("Num Threads: " + numThreads);
    	
    	if(numLines == -1) {
			try {
				numLines = EPRuntimeUtil.countLines(vf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	    	
    	generator = new PhoneCallGenerator(vf, numLines);
    	
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
 
       startThreads(numThreads, numLines, duration, cep, VoterConstants.INPUT_RATE);
       System.out.println("Total Time: " + (System.nanoTime() - startTime)/1000000l);
       System.out.println(dc.printStats());
        
    } 
}