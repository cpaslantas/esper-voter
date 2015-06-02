/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package edu.brown.benchmark.voteresper;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

import com.espertech.esper.client.EPRuntime;

import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * Utility methods for monitoring a EPRuntime instance.
 */
public class EPRuntimeUtil
{

    public static boolean awaitCompletion(EPRuntime epRuntime,
                                       int numEventsExpected,
                                       int numSecAwait,
                                       int numSecThreadSleep,
                                       int numSecThreadReport,
                                       long startTimeMSec,
                  					   EsperDataConnector dc)
    {
        System.out.println(".awaitCompletion Waiting for completion, expecting " + numEventsExpected +
                 " events within " + numSecAwait + " sec");

        int secondsWaitTotal = numSecAwait;
        long lastNumEventsProcessed = 0;
        int secondsUntilReport = 0;

        //long startTimeMSec = System.currentTimeMillis();
        long endTimeMSec = 0;
        if(System.currentTimeMillis() - startTimeMSec >= numSecAwait) {
        	return true;
        }

        while (secondsWaitTotal > 0)
        {
            try
            {
                Thread.sleep(numSecThreadSleep * 1000);
            }
            catch (InterruptedException ex)
            {
            }

            secondsWaitTotal -= numSecThreadSleep;
            secondsUntilReport += numSecThreadSleep;
            long currNumEventsProcessed = epRuntime.getNumEventsEvaluated();

            if (secondsUntilReport > numSecThreadReport)
            {
                long numPerSec = (currNumEventsProcessed - lastNumEventsProcessed) / numSecThreadReport;
                System.out.println(".awaitCompletion received=" + epRuntime.getNumEventsEvaluated() +
                         "  processed=" + currNumEventsProcessed +
                         "  perSec=" + numPerSec);
                lastNumEventsProcessed = currNumEventsProcessed;
                secondsUntilReport = 0;
            }

            // Completed loop if the total event count has been reached
            if (dc.getCompletedWorkflows() >= numEventsExpected)
            {
                endTimeMSec = System.currentTimeMillis();
                break;
            }
        }

        if (endTimeMSec == 0)
        {
            System.out.println(".awaitCompletion Not completed within " + numSecAwait + " seconds");
            return false;
        }

        long totalUnitsProcessed = epRuntime.getNumEventsEvaluated();
        long deltaTimeSec = (endTimeMSec - startTimeMSec) / 1000;

        long numPerSec = 0;
        if (deltaTimeSec > 0)
        {
            numPerSec = (totalUnitsProcessed) / deltaTimeSec;
        }
        else
        {
            numPerSec = -1;
        }

        System.out.println(".awaitCompletion Completed, sec=" + deltaTimeSec + "  avgPerSec=" + numPerSec);

        long numReceived = epRuntime.getNumEventsEvaluated();
        long numReceivedPerSec = 0;
        if (deltaTimeSec > 0)
        {
            numReceivedPerSec = (numReceived) / deltaTimeSec;
        }
        else
        {
            numReceivedPerSec = -1;
        }

        System.out.println(".awaitCompletion Runtime reports, numReceived=" + numReceived +
                 "  numProcessed=" + epRuntime.getNumEventsEvaluated() +
                 "  perSec=" +  numReceivedPerSec
                 );

        return true;
    }
    
    public static int countLines(String filename) throws IOException {
	    InputStream is = new BufferedInputStream(new FileInputStream(filename));
	    try {
	        byte[] c = new byte[1024];
	        int count = 0;
	        int readChars = 0;
	        boolean empty = true;
	        while ((readChars = is.read(c)) != -1) {
	            empty = false;
	            for (int i = 0; i < readChars; ++i) {
	                if (c[i] == '\n') {
	                    ++count;
	                }
	            }
	        }
	        return (count == 0 && !empty) ? 1 : count;
	    } finally {
	        is.close();
	    }
	}
    
    public static double nanoToSeconds(long nano) {
    	double milli = Math.round((double)nano/1000000.0);
    	return milli/1000.0;
    }
    
    public static double nanoToSeconds(double nano) {
    	double milli = Math.round(nano/1000000.0);
    	return milli/1000.0;
    }
    
    public static void writeToFile(String toWrite) {
		try {
			if(VoterConstants.WRITE_TO_FILE) {
				PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(VoterConstants.OUT_FILE, true)));
				out.println(toWrite);
				out.flush();
		    	out.close();
			}
			else {
				System.out.println(toWrite);
			}
		} 
    	
    	catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    private static final Log log = LogFactory.getLog(EPRuntimeUtil.class);
}
