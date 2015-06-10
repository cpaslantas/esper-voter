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
	
	public static final short[] areaCodes = new short[]{
		907,205,256,334,251,870,501,479,480,602,623,928,520,341,764,628,831,925,
		909,562,661,510,650,949,760,415,951,209,669,408,559,626,442,530,916,627,
		714,707,310,323,213,424,747,818,858,935,619,805,369,720,303,970,719,860,
		203,959,475,202,302,689,407,239,850,727,321,754,954,927,352,863,386,904,
		561,772,786,305,941,813,478,770,470,404,762,706,678,912,229,808,515,319,
		563,641,712,208,217,872,312,773,464,708,224,847,779,815,618,309,331,630,
		317,765,574,260,219,812,913,785,316,620,606,859,502,270,504,985,225,318,
		337,774,508,339,781,857,617,978,351,413,443,410,301,240,207,517,810,278,
		679,313,586,947,248,734,269,989,906,616,231,612,320,651,763,952,218,507,
		636,660,975,816,573,314,557,417,769,601,662,228,406,336,252,984,919,980,
		910,828,704,701,402,308,603,908,848,732,551,201,862,973,609,856,575,957,
		505,775,702,315,518,646,347,212,718,516,917,845,631,716,585,607,914,216,
		330,234,567,419,440,380,740,614,283,513,937,918,580,405,503,541,971,814,
		717,570,878,835,484,610,267,215,724,412,401,843,864,803,605,423,865,931,
		615,901,731,254,325,713,940,817,430,903,806,737,512,361,210,979,936,409,
		972,469,214,682,832,281,830,956,432,915,435,801,385,434,804,757,703,571,
		276,236,540,802,509,360,564,206,425,253,715,920,262,414,608,304,307};
		
	public static final String[] states = new String[] {
		"AK","AL","AL","AL","AL","AR","AR","AR","AZ","AZ","AZ","AZ","AZ","CA","CA",
		"CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA",
		"CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA",
		"CA","CA","CA","CA","CO","CO","CO","CO","CT","CT","CT","CT","DC","DE","FL",
		"FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL",
		"FL","FL","FL","GA","GA","GA","GA","GA","GA","GA","GA","GA","HI","IA","IA",
		"IA","IA","IA","ID","IL","IL","IL","IL","IL","IL","IL","IL","IL","IL","IL",
		"IL","IL","IL","IN","IN","IN","IN","IN","IN","KS","KS","KS","KS","KY","KY",
		"KY","KY","LA","LA","LA","LA","LA","MA","MA","MA","MA","MA","MA","MA","MA",
		"MA","MD","MD","MD","MD","ME","MI","MI","MI","MI","MI","MI","MI","MI","MI",
		"MI","MI","MI","MI","MI","MN","MN","MN","MN","MN","MN","MN","MO","MO","MO",
		"MO","MO","MO","MO","MO","MS","MS","MS","MS","MT","NC","NC","NC","NC","NC",
		"NC","NC","NC","ND","NE","NE","NH","NJ","NJ","NJ","NJ","NJ","NJ","NJ","NJ",
		"NJ","NM","NM","NM","NV","NV","NY","NY","NY","NY","NY","NY","NY","NY","NY",
		"NY","NY","NY","NY","NY","OH","OH","OH","OH","OH","OH","OH","OH","OH","OH",
		"OH","OH","OK","OK","OK","OR","OR","OR","PA","PA","PA","PA","PA","PA","PA",
		"PA","PA","PA","PA","RI","SC","SC","SC","SD","TN","TN","TN","TN","TN","TN",
		"TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX",
		"TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","UT","UT","UT","VA","VA",
		"VA","VA","VA","VA","VA","VA","VT","WA","WA","WA","WA","WA","WA","WI","WI",
		"WI","WI","WI","WV","WY"};
	    
    public static final String[] CONTESTANT_NAMES = new String[] {
    		"Jann Arden","Micah Barnes","Justin Bieber","Jim Bryson","Michael Buble",
			"Leonard Cohen","Celine Dion","Nelly Furtado","Adam Gontier","Emily Haines",
			"Avril Lavigne","Ashley Leggat","Eileen McGann","Sarah McLachlan","Joni Mitchell",
			"Mae Moore","Alanis Morissette","Emilie Mover","Anne Murray","Sam Roberts",
			"Serena Ryder","Tamara Sandor","Nicholas Scribner","Shania Twain","Neil Young",
			"Aann Jrden","Bicah Marnes","Bustin Jieber","Bim Jryson","Bichael Muble",
					"Ceonard Lohen","Deline Cion","Felly Nurtado","Gdam Aontier","Hmily Eaines",
					"Lvril Aavigne","Lshley Aeggat","Mileen EcGann","Marah ScLachlan","Moni Jitchell",
					"Nae Noore","Mlanis Aorissette","Mmilie Eover","Mnne Aurray","Ram Soberts",
					"Rerena Syder","Samara Tandor","Sicholas Ncribner","Thania Swain","Yeil Noung"
    };
	
	private EPServiceProvider cep;
	private EPRuntime cepRT;
	private int numContestants;
	private int allVotesEver;
	private long cutoffVote;
	
	public EsperTableConnector(int numContestants, EPServiceProvider cep){
		super();
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
