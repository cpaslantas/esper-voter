package edu.brown.benchmark.voteresper;

import com.espertech.esper.client.EPServiceProvider;

import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;

public class VoteSender implements Runnable{
	
	   private Thread t;
	   private PhoneCallGenerator generator;
	   private EPServiceProvider cep;
	   private EsperDataConnector dc;
	   
	   VoteSender(EPServiceProvider cep, PhoneCallGenerator pcg, EsperDataConnector dc){
	       this.cep = cep;
	       this.generator = pcg;
	       this.dc = dc;
	   }
	   public void run() {
//	      try {
	         for(int i = 0; i < 10000; i++) {
	        	PhoneCall pc = generator.receive();
	        	cep.getEPRuntime().sendEvent(pc);
	            //Thread.sleep(10);
	         }
	         System.out.println(dc.printStats());
//	     } catch (InterruptedException e) {
//	         System.out.println("VoteSender thread interrupted.");
//	     }
	   }
	   
	   public void start ()
	   {
	      if (t == null)
	      {
	         t = new Thread (this, "VoteSender");
	         t.start ();
	      }
	   }
	
}
