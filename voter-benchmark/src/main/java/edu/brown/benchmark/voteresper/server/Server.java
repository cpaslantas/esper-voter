/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package edu.brown.benchmark.voteresper.server;

import edu.brown.benchmark.voteresper.Symbols;
import edu.brown.benchmark.voteresper.VoterConstants;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * The main Esper Server thread listens on the given port.
 * It bootstrap an ESP/CEP engine (defaults to Esper) and registers EPL statement(s) into it based
 * on the given -mode argument.
 * Statements are read from an statements.properties file in the classpath
 * If statements contains '$' the '$' is replaced by a symbol string, so as to register one statement per symbol.
 * <p/>
 * Based on -queue, the server implements a direct handoff to the ESP/CEP engine, or uses a Syncrhonous queue
 * (somewhat an indirect direct handoff), or uses a FIFO queue where each events is put/take one by one from the queue.
 * Usually with few clients sending a lot of events, use the direct handoff, else consider using queues. Consumer thread
 * can be configured using -thread (it will range up to #processor x #thread).
 * When queues is full, overload policy triggers execution on the caller side.
 * <p/>
 * To simulate an ESP/CEP listener work, use -sleep.
 * <p/>
 * Use -stat to control how often percentile stats are displayed. At each display stats are reset.
 * <p/>
 * If you use -rate nxM (n threads, M event/s), the server will simulate the load for a standalone simulation without
 * any remote client(s).
 * <p/>
 * By default the benchmark registers a subscriber to the statement(s). Use -Desper.benchmark.ul to use
 * an UpdateListener instead. Note that the subscriber contains suitable update(..) methods for the default
 * proposed statement in the statements.properties files but might not be suitable if you change statements due
 * to the strong binding with statement results. 
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
public class Server extends Thread {

    private int port;
    private int threadCore;
    private int queueMax;
    private int sleepListenerMillis;
    private int statSec;
    private int simulationRate;
    private int simulationThread;
    private String mode;
    private boolean order;
    private String backend;

    public static final int DEFAULT_PORT = 6789;
    public static final int DEFAULT_THREADCORE = Runtime.getRuntime().availableProcessors();
    public static final int DEFAULT_QUEUEMAX = -1;
    public static final int DEFAULT_SLEEP = 0;
    public static final int DEFAULT_SIMULATION_RATE = -1;//-1: no simulation
    public static final int DEFAULT_SIMULATION_THREAD = -1;//-1: no simulation
    public static final int DEFAULT_STAT = 5;
    public static final String DEFAULT_MODE = "NOOP";
    public static final boolean DEFAULT_ORDER = true;
    public static final String DEFAULT_BACKEND = "voltdbadhoc";
    public static final Properties MODES = new Properties();

    private ThreadPoolExecutor executor;//can be null

    private CEPProvider.ICEPProvider cepProvider;

    public Server(String mode, int port, int threads, int queueMax, int sleep, final int statSec, boolean order, String backend, int simulationThread, final int simulationRate) {
        super("EsperServer-main");
        this.mode = mode;
        this.port = port;
        this.threadCore = threads;
        this.queueMax = queueMax;
        this.sleepListenerMillis = sleep;
        this.statSec = statSec;
        this.order = order;
        this.backend = backend;
        this.simulationThread = simulationThread;
        this.simulationRate = simulationRate;

        // turn on stat dump
        Timer t = new Timer("EsperServer-stats", true);
        t.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                StatsHolder.dump("engine");
                StatsHolder.dump("server");
                StatsHolder.dump("endToEnd");
                StatsHolder.reset();
                if (simulationRate <= 0) {
                    ClientConnection.dumpStats(statSec);
                } else {
                    SimulateClientConnection.dumpStats(statSec);
                }
            }
        }, 0L, statSec * 1000);
    }

    public void setCEPProvider(CEPProvider.ICEPProvider cepProvider) {
        this.cepProvider = cepProvider;
    }

    public synchronized void start() {
        // register ESP/CEP engine
        cepProvider = CEPProvider.getCEPProvider(mode);
        cepProvider.init(sleepListenerMillis, order, backend);

        // register statements
        String suffix = Server.MODES.getProperty("_SUFFIX");
        if ("NOOP".equals(mode)) {
            ;
        } 
        else if ("Voter".equals(mode)) {
        	
        }
        else {
            String stmtString = Server.MODES.getProperty(mode) + " " + suffix;
            System.out.println("Using " + mode + " : " + stmtString);

            if (Server.MODES.getProperty(mode).indexOf('$') < 0) {
                cepProvider.registerStatement(stmtString, mode);
                System.out.println("\nStatements registered # 1 only");
            } else {
                // create a stmt for each symbol
                for (int i = 0; i < Symbols.SYMBOLS.length; i++) {
                    if (i % 100 == 0) System.out.print(".");
                    String ticker = Symbols.SYMBOLS[i];
                    cepProvider.registerStatement(stmtString.replaceAll("\\$", ticker), mode + "-" + ticker);
                }
                System.out.println("\nStatements registered # " + Symbols.SYMBOLS.length);
            }
        }

        // start thread pool if any
        if (queueMax < 0) {
            executor = null;
            System.out.println("Using direct handoff, cpu#" + Runtime.getRuntime().availableProcessors());
        } else {
            // executor
            System.out.println("Using ThreadPoolExecutor, cpu#" + Runtime.getRuntime().availableProcessors() + ", threadCore#" + threadCore + " queue#" + queueMax);
            BlockingQueue<Runnable> queue;
            if (queueMax == 0) {
                queue = new SynchronousQueue<Runnable>(true);//enforce fairness
            } else {
                queue = new LinkedBlockingQueue<Runnable>(queueMax);
            }

            executor = new ThreadPoolExecutor(
                    threadCore,
                    threadCore,
                    //Runtime.getRuntime().availableProcessors() * threadCore,
                    10, TimeUnit.SECONDS,
                    queue,
                    new ThreadFactory() {
                        long count = 0;

                        public Thread newThread(Runnable r) {
                            System.out.println("Create EsperServer thread " + (count + 1));
                            return new Thread(r, "EsperServer-" + count++);
                        }
                    },
                    new ThreadPoolExecutor.CallerRunsPolicy() {
                        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                            super.rejectedExecution(r, e);
                        }
                    }
            );
            executor.prestartAllCoreThreads();
        }

        super.start();
    }

    public void run() {
        if (simulationRate <= 0) {
            runServer();
        } else {
            runSimulation();
        }
    }

    public void runServer() {
        try {
            System.out.println((new StringBuilder("Server accepting connections on port ")).append(port).append(".").toString());
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(new InetSocketAddress(port));
            do {
                SocketChannel socketChannel = serverSocketChannel.accept();
                System.out.println("Client connected to server.");
                (new ClientConnection(socketChannel, executor, cepProvider, statSec)).start();
            } while (true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void runSimulation() {
        System.out.println("Server in sumulation mode with event/s "
                + simulationThread + " x " + simulationRate
                + " = " + simulationThread * simulationRate
        );
        SimulateClientConnection[] sims = new SimulateClientConnection[simulationThread];
        for (int i = 0; i < sims.length; i++) {
            sims[i] = new SimulateClientConnection(simulationRate, executor, cepProvider, statSec, VoterConstants.VOTE_DIR + VoterConstants.VOTE_FILE, VoterConstants.DURATION + VoterConstants.WARMUP_DURATION);
            sims[i].start();
        }

        try {
            for (SimulateClientConnection sim : sims) {
                sim.join();
            }
        } catch (InterruptedException e) {
            ;
        }
    }

    public static void main(String argv[]) throws IOException {
    	SimpleLayout layout = new SimpleLayout();
        ConsoleAppender appender = new ConsoleAppender(new SimpleLayout());
        Logger.getRootLogger().addAppender(appender);
    	
        // load modes
        MODES.load(Server.class.getClassLoader().getResourceAsStream("statements.properties"));
        MODES.put("NOOP", "");
        //MODES.put("noorder", "");

        int port = DEFAULT_PORT;
        int threadCore = DEFAULT_THREADCORE;
        int queueMax = DEFAULT_QUEUEMAX;
        int sleep = DEFAULT_SLEEP;
        int simulationRate = DEFAULT_SIMULATION_RATE;
        int simulationThread = DEFAULT_SIMULATION_THREAD;
        String mode = DEFAULT_MODE;
        int stats = DEFAULT_STAT;
        boolean order = DEFAULT_ORDER;
        String backend = DEFAULT_BACKEND;
        for (int i = 0; i < argv.length; i++)
            if ("-port".equals(argv[i])) {
                i++;
                port = Integer.parseInt(argv[i]);
            } else if ("-thread".equals(argv[i])) {
                i++;
                threadCore = Integer.parseInt(argv[i]);
                VoterConstants.NUM_THREADS = threadCore;
            } else if ("-queue".equals(argv[i])) {
                i++;
                queueMax = Integer.parseInt(argv[i]);
            } else if ("-sleep".equals(argv[i])) {
                i++;
                sleep = Integer.parseInt(argv[i]);
            } else if ("-stat".equals(argv[i])) {
                i++;
                stats = Integer.parseInt(argv[i]);
            } else if ("-mode".equals(argv[i])) {
                i++;
                mode = argv[i];
                if (MODES.getProperty(mode) == null) {
                    System.err.println("Unknown mode");
                    printUsage();
                }
            } else if ("-rate".equals(argv[i])) {
                i++;
                int xIndex = argv[i].indexOf('x');
                simulationThread = Integer.parseInt(argv[i].substring(0, xIndex));
                simulationRate = Integer.parseInt(argv[i].substring(xIndex + 1));
                VoterConstants.INPUT_RATE = simulationRate;
            } else if ("-order".equals(argv[i])) {
            	i++;
            	if("false".equals(argv[i])) {
            		order = false;
            		VoterConstants.ORDER = "false";
            	}
            	else {
            		order = true;
            		VoterConstants.ORDER = "true";
            	}
            
    		} else if ("-log".equals(argv[i])) {
            	i++;
            	if("false".equals(argv[i])) {
            		Logger.getLogger(VoterConstants.COMMAND_LOG).setLevel((Level) Level.WARN);
            		VoterConstants.LOG = "false";
            	}
            	else {
            		Logger.getLogger(VoterConstants.COMMAND_LOG).setLevel((Level) Level.DEBUG);
            		VoterConstants.LOG = "true";
            	}
            
    		} else if ("-backend".equals(argv[i])) {
            	i++;
            	backend = argv[i];  
            	VoterConstants.BACKEND = backend;
    		} else if ("-threshold".equals(argv[i])) {
                i++;
                VoterConstants.VOTE_THRESHOLD = Integer.parseInt(argv[i]);
            } else if ("-contestants".equals(argv[i])) {
                i++;
                VoterConstants.NUM_CONTESTANTS = Integer.parseInt(argv[i]);
            } else if ("-dir".equals(argv[i])) {
            	i++;
            	VoterConstants.changeRootDir(argv[i]);
    		} else if ("-file".equals(argv[i])) {
            	i++;
            	VoterConstants.VOTE_FILE = argv[i];            
    		} else if ("-outfile".equals(argv[i])) {
            	i++;
            	VoterConstants.OUT_FILE = argv[i];            
    		} else if ("-duration".equals(argv[i])) {
                i++;
                VoterConstants.DURATION = Integer.parseInt(argv[i]);
            } else {
                printUsage();
            }
        
        System.out.println("THREAD COUNT: " + threadCore + ", MODE: " + mode);

        Server bs = new Server(mode, port, threadCore, queueMax, sleep, stats, order, backend, simulationThread, simulationRate);
        bs.start();
        try {
            bs.join();
        } catch (InterruptedException e) {
            ;
        }
    }

    private static void printUsage() {
        System.err.println("usage: com.espertech.esper.example.benchmark.server.Server <-port #> <-thread #> <-queue #> <-sleep #> <-stat #> <-rate #x#> <-mode xyz>");
        System.err.println("defaults:");
        System.err.println("  -port:    " + DEFAULT_PORT);
        System.err.println("  -thread:  " + DEFAULT_THREADCORE);
        System.err.println("  -queue:   " + DEFAULT_QUEUEMAX + "(-1: no executor, 0: SynchronousQueue, n: LinkedBlockingQueue");
        System.err.println("  -sleep:   " + DEFAULT_SLEEP + "(no sleep)");
        System.err.println("  -stat:   " + DEFAULT_STAT + "(s)");
        System.err.println("  -rate:    " + DEFAULT_SIMULATION_RATE + "(no standalone simulation, else <n>x<evt/s> such as 2x1000)");
        System.err.println("  -mode:    " + "(default " + DEFAULT_MODE + ", choose from " + MODES.keySet().toString() + ")");
        System.err.println("  -order:    " + "(default " + DEFAULT_ORDER + ")");
        System.err.println("Modes are read from statements.properties in the classpath");
        System.exit(1);
    }

}
