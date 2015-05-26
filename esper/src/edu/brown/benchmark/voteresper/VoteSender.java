package edu.brown.benchmark.voteresper;

import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.espertech.esper.client.EPServiceProvider;

import edu.brown.benchmark.voteresper.dataconnectors.EsperDataConnector;

public class VoteSender implements Runnable{
	
	   private PhoneCallGenerator generator;
	   private EPServiceProvider cep;
	   private EsperDataConnector dc;
	   
	   VoteSender(EPServiceProvider cep, PhoneCallGenerator pcg, EsperDataConnector dc){
	       this.cep = cep;
	       this.generator = pcg;
	       this.dc = dc;
	   }
	   
	   public boolean hasVotes() {
		   return generator.hasVotes();
	   }
	   
	   public void run() {
		   
		  PhoneCall pc = generator.receive();
       	  cep.getEPRuntime().sendEvent(pc);
	   }

}
