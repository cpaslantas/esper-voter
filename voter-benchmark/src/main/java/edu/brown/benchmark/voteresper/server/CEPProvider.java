/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package edu.brown.benchmark.voteresper.server;

import com.espertech.esper.client.*;

import edu.brown.benchmark.voteresper.*;

/**
 * A factory and interface to wrap ESP/CEP engine dependency in a single space
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
public class CEPProvider {

    public static interface ICEPProvider {

        public void init(int sleepListenerMillis, boolean order);

        public void registerStatement(String statement, String statementID);

        public void sendEvent(Object theEvent);
    }

    public static ICEPProvider getCEPProvider() {
        String className = System.getProperty("esper.benchmark.provider", EsperCEPProvider.class.getName());
        System.out.println("CLASS NAME: " + className);
        try {
            Class klass = Class.forName(className);
            return (ICEPProvider) klass.newInstance();
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        }
    }
    
    public static ICEPProvider getCEPProvider(String type) {
    	String className = System.getProperty("esper.benchmark.provider", EsperCEPProvider.class.getName());
    	if(type.equals(VoterConstants.VOTER_TYPE))
    		className = System.getProperty("esper.benchmark.provider", VoterCEPProvider.class.getName());
        
        try {
            Class klass = Class.forName(className);
            return (ICEPProvider) klass.newInstance();
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        }
    }

    public static class EsperCEPProvider implements ICEPProvider {

        private EPAdministrator epAdministrator;

        private EPRuntime epRuntime;

        // only one of those 2 will be attached to statement depending on the -mode selected
        private UpdateListener updateListener;
        private MySubscriber subscriber;

        private static int sleepListenerMillis;

        public EsperCEPProvider() {
        }

        public void init(final int _sleepListenerMillis, boolean order) {
            sleepListenerMillis = _sleepListenerMillis;
            Configuration configuration;

            // EsperHA enablement - if available
            try {
                Class configurationHAClass = Class.forName("com.espertech.esperha.client.ConfigurationHA");
                configuration = (Configuration) configurationHAClass.newInstance();
                System.out.println("=== EsperHA is available, using ConfigurationHA ===");
            } catch (ClassNotFoundException e) {
                configuration = new Configuration();
            } catch (Throwable t) {
                System.err.println("Could not properly determine if EsperHA is available, default to Esper");
                t.printStackTrace();
                configuration = new Configuration();
            }
            configuration.addEventType("Market", MarketData.class);


            // EsperJMX enablement - if available
			try {
				Class.forName("com.espertech.esper.jmx.client.EsperJMXPlugin");
	            configuration.addPluginLoader(
	                    "EsperJMX",
	                    "com.espertech.esper.jmx.client.EsperJMXPlugin",
	    				null);// will use platform mbean - should enable platform mbean connector in startup command line
                System.out.println("=== EsperJMX is available, using platform mbean ===");
			} catch (ClassNotFoundException e) {
				;
			}
			
			//REMOVES ORDER
			if(!order) {
				configuration.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false); //removes order-preserving
				configuration.getEngineDefaults().getThreading().setInsertIntoDispatchPreserveOrder(false);
				configuration.getEngineDefaults().getThreading()
		          .setListenerDispatchPreserveOrder(false);
				configuration.getEngineDefaults().getThreading()
		          .setInternalTimerEnabled(false);   // remove thread that handles time advancing
			}
			//END REMOVE ORDER


            EPServiceProvider epService = EPServiceProviderManager.getProvider("benchmark", configuration);
            epAdministrator = epService.getEPAdministrator();
            updateListener = new MyUpdateListener();
            subscriber = new MySubscriber();
            epRuntime = epService.getEPRuntime();
        }

        public void registerStatement(String statement, String statementID) {
            EPStatement stmt = epAdministrator.createEPL(statement, statementID);
            if (System.getProperty("esper.benchmark.ul") != null) {
                stmt.addListener(updateListener);
            } else {
                stmt.setSubscriber(subscriber);
            }
        }

        public void sendEvent(Object theEvent) {
            epRuntime.sendEvent(theEvent);
        }
    }

    public static class MyUpdateListener implements UpdateListener {
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null) {
                if (EsperCEPProvider.sleepListenerMillis > 0) {
                    try {
                        Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                    } catch (InterruptedException ie) {
                        ;
                    }
                }
            }
        }
    }

    public static class MySubscriber {
        public void update(String ticker) {
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }

        public void update(MarketData marketData) {
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }

        public void update(String ticker, double avg, long count, double sum) {
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    }

}
