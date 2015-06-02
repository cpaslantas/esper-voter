package edu.brown.benchmark.voteresper;

import com.espertech.esper.client.*;

import edu.brown.benchmark.voteresper.dataconnectors.*;
import edu.brown.benchmark.voteresper.listeners.*;
import edu.brown.benchmark.voteresper.tuples.PhoneCall;
import edu.brown.benchmark.voteresper.tuples.Vote;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
 
public class VoterMain {
 
    private static PhoneCallGenerator generator;
    private static EsperDataConnector dc;
    private static StatsCollector stats;
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
		long startTimeMillis = System.currentTimeMillis();
		System.out.println("entering loop - ticksPerMS = " + ticksPerMS);
		while(vs.hasVotes() && ticksSent < totalNumTicks)
		{
			if(System.nanoTime() - startTime > numberOfNanoSeconds) {
				break;
			}
			curDuration = System.nanoTime() - curTime;
			
			int timesToExecute = totalNumTicks;
			if(inputRate > 0)
				timesToExecute = (int)((ticksPerMS * (double)curDuration)/1000000.0);
			if(timesToExecute > 0)
				curTime = System.nanoTime();
			for(int i = 0; i < timesToExecute; i++) {
				if(!vs.hasVotes() || ticksSent >= totalNumTicks){
					break;
				}
				pool.execute(vs);
				ticksSent++;
			}
		}
		long endTimeMillis = System.currentTimeMillis();
		System.out.println("All "+ totalNumTicks + " tuples queued in " + (endTimeMillis - startTimeMillis) + "ms");
		
		System.out.println(".performTest Listening for completion");
		EPRuntimeUtil.awaitCompletion(epService.getEPRuntime(), totalNumTicks, numberOfSecondsWaitForCompletion, 1, 10, startTimeMillis, dc);
		
		pool.shutdown();
	}
    
 
    public static void main(String[] args) {
    	
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
    			VoterConstants.NUM_THREADS = new Integer(value);
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
    		else if(param.equals("-numlines") || param.equals("-nl")) {
    			VoterConstants.NUM_LINES = new Integer(value);
    		}
    		else if(param.equals("-duration") || param.equals("-d")) {
    			VoterConstants.DURATION = new Integer(value);
    		}
    		else if(param.equals("-contestants") || param.equals("-nc")) {
    			VoterConstants.NUM_CONTESTANTS = new Integer(value);
    		}
    		else if(param.equals("-delthreshold") || param.equals("-dt")) {
    			VoterConstants.VOTE_THRESHOLD = new Integer(value);
    		}
    		else if(param.equals("-noorder") || param.equals("-no")) {
    			if(value.equals("true"))
    				VoterConstants.NO_ORDER = true;
    		}
    		else if(param.equals("-outfile") || param.equals("-of")) {
    			VoterConstants.OUT_FILE = value;
    		}
    	}
    	int cores = Runtime.getRuntime().availableProcessors();
    	System.out.println("NUMBER OF CORES: " + cores);

    	String vf = VoterConstants.VOTE_DIR + VoterConstants.VOTE_FILE;
    	
    	if(VoterConstants.NUM_LINES == -1) {
			try {
				VoterConstants.NUM_LINES = EPRuntimeUtil.countLines(vf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	System.out.println(VoterConstants.getConfiguration());
    	    	
    	generator = new PhoneCallGenerator(vf, VoterConstants.NUM_LINES);
    	stats = new StatsCollector();
    	
    	//The Configuration is meant only as an initialization-time object.
        Configuration cepConfig = new Configuration();
        //configuration changes
        if(VoterConstants.NO_ORDER) {
        	cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false); //removes order-preserving
        	cepConfig.getEngineDefaults().getThreading().setInsertIntoDispatchPreserveOrder(false);
        }
        //end configuration changes
        
        cepConfig.addEventType("PhoneCall", PhoneCall.class.getName());
        cepConfig.addEventType("Vote", Vote.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider("VoterDemo", cepConfig);
        EPRuntime cepRT = cep.getEPRuntime();
        
        dc = new EsperTableConnector(VoterConstants.NUM_CONTESTANTS, cep, stats);
 
        EPAdministrator cepAdm = cep.getEPAdministrator();
        
        EPStatement phoneCallStatement = cepAdm.createEPL("select * from " +
                "PhoneCall(contestantNumber>0)");
//        EPStatement voteWindowStmt = cepAdm.createEPL("select * from " +
//                "Vote.win:length_batch(" + VoterConstants.WIN_SLIDE + ")");
//        EPStatement voteDeleteStmt = cepAdm.createEPL("select * from " +
//                "Vote.win:length_batch(" + VoterConstants.VOTE_THRESHOLD + ")");
//        EPStatement voteStmt = cepAdm.createEPL("select * from " +
//                "Vote");
        
        phoneCallStatement.addListener(new PhoneCallListener(cep, dc));
//        voteWindowStmt.addListener(new VoteWindowListener(cep, dc));
//        voteDeleteStmt.addListener(new VoteDeleteListener(cep, dc));
//        voteStmt.addListener(new WorkflowEndListener(cep, dc));
        
        System.out.println("VOTER MAIN");
 
       startThreads(VoterConstants.NUM_THREADS, VoterConstants.NUM_LINES, VoterConstants.DURATION, cep, VoterConstants.INPUT_RATE);
       System.out.println("Total Time: " + (System.nanoTime() - startTime)/1000000l);
       System.out.println(dc.printStats());
       EPRuntimeUtil.writeToFile(VoterConstants.getConfiguration());
       EPRuntimeUtil.writeToFile(stats.getStats());
       System.exit(0);
    } 
}