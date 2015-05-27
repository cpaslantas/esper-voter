/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.											   *
 *  Ported By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)  *								   								   
 *                                                                         *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

package edu.brown.benchmark.voteresper;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Queue;

public class PhoneCallGenerator {
	
	private class QueueLoader implements Runnable {
		
		private Queue<PhoneCall> callQueue;
		private String voteFile;
		private boolean stop = false;
		private int queueSize;
		
		public QueueLoader(String filename, Queue<PhoneCall> cq, int queueSize) {
			voteFile = filename;
			callQueue = cq;
			this.queueSize = queueSize;
		}
		
		public void stop() {
			stop = true;
		}
		
		public void run() {
			try {
				in = new BufferedReader(new FileReader(voteFile));
				String s = in.readLine();
				while(!stop && s != null) {
					while(callQueue.size() < queueSize) {
						if(s == null)
							break;
						callQueue.add(new PhoneCall(s));
						s = in.readLine();
					}
					Thread.sleep(VoterConstants.SLEEP_TIME);
				}
				in.close();
				stop();
			}
			catch (UnknownHostException e){
				System.err.println("UnknownHostException");
				e.printStackTrace();
			}
			catch (IOException e) {
				System.err.println("IOException");
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private String voteFile;
	private Queue<PhoneCall> callQueue;
    private BufferedReader in;
    private int numLines;
	
	public void loadAllCalls() {
		try {
			in = new BufferedReader(new FileReader(voteFile));
			String s = in.readLine();
			while(s != null) {
				callQueue.add(new PhoneCall(s));
				s = in.readLine();
			}
			in.close();
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
	
	public PhoneCallGenerator(String vf, int numLines) {
		callQueue = new LinkedList<PhoneCall>();
		voteFile = vf;
		this.numLines = numLines;
		QueueLoader ql = new QueueLoader(voteFile, callQueue, VoterConstants.QUEUE_SIZE);
		Thread t = new Thread(ql);
		t.start();
    }
	
	public boolean hasVotes() {
		return callQueue.size() > 0;
	}
	
	public PhoneCall receive()
	{
		if(callQueue.isEmpty())
			return null;
		PhoneCall out = callQueue.poll();
		out.startTime = System.nanoTime();
		out.tupleStartTime = out.startTime;
		
		return out;
	}

}
