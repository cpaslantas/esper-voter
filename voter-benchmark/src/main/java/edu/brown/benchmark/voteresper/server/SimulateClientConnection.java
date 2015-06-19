/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package edu.brown.benchmark.voteresper.server;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ThreadPoolExecutor;

import edu.brown.benchmark.voteresper.EPRuntimeUtil;
import edu.brown.benchmark.voteresper.MarketData;
import edu.brown.benchmark.voteresper.Symbols;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.tuples.PhoneCall;

/**
 * A thread started by the Server when running in simulation mode.
 * It acts as ClientConnection
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
public class SimulateClientConnection extends Thread {

    static Map<Integer, SimulateClientConnection> CLIENT_CONNECTIONS = Collections.synchronizedMap(new HashMap<Integer, SimulateClientConnection>());

    public static void dumpStats(int statSec) {
        long totalCount = 0;
        int cnx = 0;
        SimulateClientConnection any = null;
        for (SimulateClientConnection m : CLIENT_CONNECTIONS.values()) {
            cnx++;
            totalCount += m.countLast10sLast;
            any = m;
        }
        if (any != null) {
            System.out.printf("Throughput %.0f (active %d pending %d)\n",
                    (float) totalCount / statSec,
                    any.executor == null ? 0 : any.executor.getCorePoolSize(),
                    any.executor == null ? 0 : any.executor.getQueue().size()
            );
        }
    }

    private int simulationRate;
    private CEPProvider.ICEPProvider cepProvider;
    private ThreadPoolExecutor executor;
    private final int statSec;
    private long countLast10sLast = 0;
    private long countLast10s = 0;
    private long lastThroughputTick = System.currentTimeMillis();
    private long countForStatSec = 0;
    private long countForStatSecLast = 0;
    private int myID;
    private static int ID = 0;
    private Queue<PhoneCall> callQueue;
    private String voteFile;
    private final int DURATION_MS;
    private int numLines;

    public SimulateClientConnection(int simulationRate, ThreadPoolExecutor executor, CEPProvider.ICEPProvider cepProvider, int statSec, String vf, int duration) {
        super("EsperServer-cnx-" + ID++);
        this.simulationRate = simulationRate;
        this.executor = executor;
        this.cepProvider = cepProvider;
        this.statSec = statSec;
        this.voteFile = vf;
        this.DURATION_MS = duration;
        myID = ID - 1;

        // simulationRate event / s
        // 10ms ~ simulationRate / 1E2
        CLIENT_CONNECTIONS.put(myID, this);
        callQueue = new LinkedList<PhoneCall>();
        try {
			this.numLines = EPRuntimeUtil.countLines(vf);
		} catch (IOException e) {
			this.numLines = -1;
			e.printStackTrace();
		}
        loadAllCalls();
    }
    
    public void loadAllCalls() {
		try {
			System.out.println(voteFile);
			BufferedReader in = new BufferedReader(new FileReader(voteFile));
			String s = in.readLine();
			while(s != null) {
				callQueue.add(new PhoneCall(s));
				s = in.readLine();
			}
			in.close();
			System.out.println("LOADED ALL VOTES");
		}
		catch (UnknownHostException e){
			System.err.println("UnknownHostException");
			e.printStackTrace();
		}
		catch (IOException e) {
			System.err.println("IOException");
			e.printStackTrace();
		}
	}
    
    public void run() {
	    int eventPer50ms = simulationRate / 20;
	    int countLast5s = 0;
	    int sleepLast5s = 0;
	    long lastThroughputTick = System.currentTimeMillis();
	    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(PhoneCall.SIZE / 8);
	    long startTime = System.currentTimeMillis();
	    try {
	        do {
	        	if(System.currentTimeMillis() - startTime >= DURATION_MS)
	        		break;
	            long ms = System.currentTimeMillis();
	            for (int i = 0; i < eventPer50ms; i++) {
	                if(callQueue.isEmpty())
	                	break;
	                final PhoneCall pc = callQueue.poll();
	                if (executor == null) {
                        long ns = System.nanoTime();
                        cepProvider.sendEvent(pc);
                        StatsHolder.getEngine().update(System.nanoTime() - ns);
                    } else {
                        executor.execute(new Runnable() {
                            public void run() {
                                long ns = System.nanoTime();
                                cepProvider.sendEvent(pc);
                                long nsDone = System.nanoTime();
                                long msDone = System.currentTimeMillis();
                                StatsHolder.getEngine().update(nsDone - ns);
                                StatsHolder.getServer().update(nsDone - pc.getInTime());
                                StatsHolder.getEndToEnd().update(msDone - pc.getTime());
                            }
                        });
                    }
	
	                countLast5s++;
	
	                // info
	                if (System.currentTimeMillis() - lastThroughputTick > 5 * 1E3) {
	                    System.out.printf("Sent %d in %d(ms) avg ns/msg %.0f(ns) avg %d(msg/s) sleep %d(ms)\n",
	                            countLast5s,
	                            System.currentTimeMillis() - lastThroughputTick,
	                            (float) 1E6 * countLast5s / (System.currentTimeMillis() - lastThroughputTick),
	                            countLast5s / 5,
	                            sleepLast5s
	                    );
	                    countLast5s = 0;
	                    sleepLast5s = 0;
	                    lastThroughputTick = System.currentTimeMillis();
	                }
	            }
	            //stats
                countForStatSec++;
                if (System.currentTimeMillis() - lastThroughputTick > statSec * 1E3) {
                    countForStatSecLast = countForStatSec;
                    countForStatSec = 0;
                    lastThroughputTick = System.currentTimeMillis();
                }
	
	            // rate adjust
	            if (System.currentTimeMillis() - ms < 50) {
	                // lets avoid sleeping if == 1ms, lets account 3ms for interrupts
	                long sleep = Math.max(1, (50 - (System.currentTimeMillis() - ms) - 3));
	                sleepLast5s += sleep;
	                Thread.sleep(sleep);
	            }
	        } while (!callQueue.isEmpty());
	    } catch (Throwable t) {
	        t.printStackTrace();
	        System.err.println("Error sending data to server. Did server disconnect?");
	    } finally {
	//        StatsHolder.dump("engine");
	//        StatsHolder.dump("server");
	//        StatsHolder.dump("endToEnd");
	        ClientConnection.dumpStats(statSec);
	        CLIENT_CONNECTIONS.remove(myID);
	        StatsHolder.remove(StatsHolder.getEngine());
	        StatsHolder.remove(StatsHolder.getServer());
	        StatsHolder.remove(StatsHolder.getEndToEnd());
	        EPRuntimeUtil.writeToFile(VoterConstants.getConfiguration());
	        EPRuntimeUtil.writeToFile(cepProvider.getStatsCollector().getStats());
	        System.exit(0);
	    }
    }
}
