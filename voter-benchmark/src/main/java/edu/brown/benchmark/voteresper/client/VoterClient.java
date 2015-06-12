
/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package edu.brown.benchmark.voteresper.client;

import edu.brown.benchmark.voteresper.EPRuntimeUtil;
import edu.brown.benchmark.voteresper.MarketData;
import edu.brown.benchmark.voteresper.Symbols;
import edu.brown.benchmark.voteresper.tuples.PhoneCall;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;

/**
 * A thread that sends market data (symbol, volume, price) at the target rate to the remote host
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
public class VoterClient extends Thread {

    private Client client;
    private Queue<PhoneCall> callQueue;
    private String voteFile;
    private int numLines;
    private final int DURATION_MS;

    public VoterClient(Client client, String vf, int duration) {
        this.client = client;
        this.voteFile = vf;
        this.DURATION_MS = duration;
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
        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open(new InetSocketAddress(client.host, client.port));
            System.out.printf("Client connected to %s:%d, rate %d msg/s\n", client.host, client.port, client.rate);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        int eventPer50ms = client.rate / 20;
        int tickerIndex = 0;
        int countLast5s = 0;
        int sleepLast5s = 0;
        long lastThroughputTick = System.currentTimeMillis();
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(PhoneCall.SIZE / 8);
        long startTime = System.currentTimeMillis();
        try {
            do {
                long ms = System.currentTimeMillis();
                for (int i = 0; i < eventPer50ms; i++) {
                    tickerIndex = tickerIndex % Symbols.SYMBOLS.length;
                    if(callQueue.isEmpty())
                    	break;
                    PhoneCall pc = callQueue.poll();

                    byteBuffer.clear();
                    pc.toByteBuffer(byteBuffer);
                    byteBuffer.flip();
                    socketChannel.write(byteBuffer);

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

                // rate adjust
                if (System.currentTimeMillis() - ms < 50) {
                    // lets avoid sleeping if == 1ms, lets account 3ms for interrupts
                    long sleep = Math.max(1, (50 - (System.currentTimeMillis() - ms) - 3));
                    sleepLast5s += sleep;
                    Thread.sleep(sleep);
                }
            } while (!callQueue.isEmpty());// && System.currentTimeMillis() - startTime < DURATION_MS);
        } catch (Throwable t) {
            t.printStackTrace();
            System.err.println("Error sending data to server. Did server disconnect?");
        } finally {
        	try {
				socketChannel.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }
}