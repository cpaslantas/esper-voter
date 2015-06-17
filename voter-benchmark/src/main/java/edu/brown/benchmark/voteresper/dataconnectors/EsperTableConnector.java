package edu.brown.benchmark.voteresper.dataconnectors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPOnDemandQueryResult;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventBean;

import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.tuples.Vote;

public class EsperTableConnector extends EsperDataConnector {
	
	private EPServiceProvider cep;
	private EPRuntime cepRT;
	private int numContestants;
	private int allVotesEver;
	private long cutoffVote;
	
	public EsperTableConnector(int numContestants, EPServiceProvider cep, StatsCollector stats){
		super(stats);
		this.numContestants = numContestants;
		this.cep = cep;
		this.cepRT = cep.getEPRuntime();
		this.allVotesEver = 0;
		this.cutoffVote = 0;
		initializeDatabase();
		populateDatabase(numContestants);
	}
	
	private void initializeDatabase() {
		EPAdministrator cepAdm = cep.getEPAdministrator();
		cepAdm.createEPL("create table votes_tbl (vote_id long primary key, " +
				 " contestant_number  int, " +
				 "phone_number long, " + 
				 " state string, " +
				 "created	     long )");
		cepAdm.createEPL("create table contestants (" +
				 " contestant_number int primary key," +
				 " contestant_name string)");
		cepAdm.createEPL("create table area_code_state (" +
				 "area_code int primary key, " +
				 "state string)");
		cepAdm.createEPL("create table leaderboard_tbl (vote_id long primary key, " +
				 " contestant_number  int, " +
				 "phone_number long, " + 
				 " state string, " +
				 "created	     long )");
	}
	
	private void populateDatabase(int numContestants) {
		for(int i = 0; i < numContestants; i++) {
			cepRT.executeQuery("insert into contestants values (" + (i+1) + ",'" + CONTESTANT_NAMES[i] + "')");
		}
		assert areaCodes.length == states.length;
		for(int i = 0; i < areaCodes.length; i++) {
			cepRT.executeQuery("insert into area_code_state values (" + areaCodes[i] + ",'" + states[i] + "')");
		}
	}

	@Override
	public boolean hasVoted(long phoneNumber) {
		return numTimesVoted(phoneNumber) > 0;
	}

	@Override
	public boolean realContestant(int contestant) {
		EPOnDemandQueryResult result = cepRT.executeQuery("select contestant_number from contestants where contestant_number = " + contestant); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return false;
		else
			return true;
	}

	@Override
	public long numTimesVoted(long phoneNumber) {
		EPOnDemandQueryResult result = cepRT.executeQuery("select count(*) as num_votes from votes_tbl where phone_number = " + phoneNumber); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return -1;
		
		return (Long)e[0].get("num_votes");
	}

	@Override
	public String getState(long phoneNumber) {
		short areaCode = (short)(phoneNumber/10000000l);
		EPOnDemandQueryResult result = cepRT.executeQuery("select state from area_code_state where area_code = " + areaCode); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return "XX";
		
		return (String)e[0].get("state");
	}
	
	public long getNumRemainingContestants() {
		EPOnDemandQueryResult result = cepRT.executeQuery("select count(*) as num_contestants from contestants"); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return -1;
		
		return (Long)e[0].get("num_contestants");
	}

	@Override
	public long getNumVotes() {
		EPOnDemandQueryResult result = cepRT.executeQuery("select count(*) as num_votes from votes_tbl"); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return -1;
		
		return (Long)e[0].get("num_votes");
	}
	
	@Override
	public int getAllVotesEver() {
		return allVotesEver;
	}

	@Override
	public boolean insertVote(Vote v) {
		cepRT.executeQuery("insert into votes_tbl values (" + v.outputValues() + ")"); 					
		allVotesEver++;
		return true;
	}
	
	@Override
	public long getLeaderboardSize() {
		EPOnDemandQueryResult result = cepRT.executeQuery("select count(*) as num_votes from leaderboard_tbl"); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return 0;
		
		return (Long)e[0].get("num_votes");
	}
	
	@Override
	public long getCutoffVote() {
		return cutoffVote;
	}
	
	@Override
	public void setCutoffVote(long cutoff) {
		cutoffVote = cutoff;
	}
	
	@Override
	public boolean deleteCutoff(long cutoff) {
		cepRT.executeQuery("delete from leaderboard_tbl where vote_id <= " + cutoff);
		return true;
	}

	@Override
	public boolean insertLeaderboard(Vote v) {
		cepRT.executeQuery("insert into leaderboard_tbl values (" + v.outputValues() + ")");
		return true;
	}

	@Override
	public int findLowestContestant() {
		EPOnDemandQueryResult result = cepRT.executeQuery("select contestant_number, count(*) as num_votes from votes_tbl group by contestant_number order by num_votes, contestant_number desc"); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return -1;
		
		return (Integer)e[0].get("contestant_number");
	}
	
	@Override
	public int findTopContestant() {
		EPOnDemandQueryResult result = cepRT.executeQuery("select contestant_number, count(*) as num_votes from votes_tbl group by contestant_number order by num_votes desc, contestant_number asc"); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return -1;
		
		return (Integer)e[0].get("contestant_number");
	}
	
	@Override
	public boolean removeVotes(int contestant) {
		cepRT.executeQuery("delete from votes_tbl where contestant_number = " + contestant);
		return true;
	}

	@Override
	public boolean removeContestant(int contestant) {
		//System.out.println(printAllVotes());
		System.out.println("REMOVING CONTESTANT " + contestant);
		//cepRT.executeQuery("delete from votes_tbl where contestant_number = " + contestant);
		cepRT.executeQuery("delete from contestants where contestant_number = " + contestant);
		numContestants--;
		return true;
	}
	
	public String printAllVotes() {
		EPOnDemandQueryResult result = cepRT.executeQuery("select contestant_number, count(*) as num_votes from votes_tbl group by contestant_number order by num_votes, contestant_number desc"); 
		EventBean[] e = result.getArray();
		
		if(e.length == 0)
			return "";
		
		String o = "ALL VOTES: " + allVotesEver + "\n";
		o += "CUR VOTES: " + getNumVotes() + "\n";
		for(int i = 0; i < e.length; i++) {
			o += e[i].get("contestant_number") + "," + e[i].get("num_votes") + "\n";
		}
		o += "-----------\n";
		return o;		
	}
	
	public String printStats(){
		String out = "Total Num Votes: " + allVotesEver + "\n";
		out += "Current Leader: " + findTopContestant() + "\n";
		out += "Current Loser: " + findLowestContestant() + "\n";
		out += "Remaining Contestants: " + getNumRemainingContestants();
		return out;
	}
}
