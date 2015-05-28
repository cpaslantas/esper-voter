package edu.brown.benchmark.voteresper.dataconnectors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.tuples.Vote;

public class DummyDataConnector extends EsperDataConnector {
	
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
	
	private HashMap<Long, Vote> votes;
	private HashMap<Integer, Set<Vote>> votesByContestant;
	private HashMap<Short, String> stateAreaCode;
	private int numContestants;
	private int totalNumVotes;
	private int allVotesEver;
	private long cutoffVote;
	
	public DummyDataConnector(int numContestants, StatsCollector stats){
		super(stats);
		this.numContestants = numContestants;
		votes = new HashMap<Long, Vote>();
		votesByContestant = new HashMap<Integer, Set<Vote>>();
		for(int i = 0; i < numContestants; i++) {
			votesByContestant.put(i+1, new HashSet<Vote>());
		}
		totalNumVotes = 0;
		allVotesEver = 0;
		cutoffVote = 0;
		populateAreaCodes();
	}
	
	private void populateAreaCodes() {
		assert areaCodes.length == states.length;
		stateAreaCode = new HashMap<Short, String>();
		for(int i = 0; i < areaCodes.length; i++) {
			stateAreaCode.put(areaCodes[i], states[i]);
		}
	}

	@Override
	public boolean hasVoted(long phoneNumber) {
		return votes.containsKey(phoneNumber);
	}

	@Override
	public boolean realContestant(int contestant) {
		if(contestant < 1)
			return false;
		return contestant <= numContestants;
	}

	@Override
	public long numTimesVoted(long phoneNumber) {
		if(votes.containsKey(phoneNumber))
			return 1;
		else
			return 0;
	}

	@Override
	public String getState(long phoneNumber) {
		short areaCode = (short)(phoneNumber/10000000l);
		if(!stateAreaCode.containsKey(areaCode))
			return "XX";
		else
			return stateAreaCode.get(areaCode);
	}
	
	public long getNumRemainingContestants() {
		return (long)votesByContestant.keySet().size();
	}

	@Override
	public long getNumVotes() {
		return (long)totalNumVotes;
	}
	
	@Override
	public int getAllVotesEver() {
		return allVotesEver;
	}

	@Override
	public boolean insertVote(Vote v) {
		if(votes.containsKey(v.phoneNumber))
			return false;
		
		int contestantId = v.contestantNumber;
		votes.put(v.phoneNumber, v);
		votesByContestant.get(contestantId).add(v);
		totalNumVotes++;
		allVotesEver++;
		return true;
	}
	
	public long getLeaderboardSize(){
		return -1;
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
		//cepRT.executeQuery("delete from leaderboard_tbl where vote_id <= " + cutoff);
		return true;
	}

	@Override
	public boolean insertLeaderboard(Vote v) {
		//cepRT.executeQuery("insert into leaderboard_tbl values (" + v.outputValues() + ")");
		return false;
	}
	@Override
	public int findLowestContestant() {
		int lowest = -1;
		int numVotes = Integer.MAX_VALUE;
		for(int conNum : votesByContestant.keySet()){
			int curVotes = votesByContestant.get(conNum).size();
			if(curVotes < numVotes) {
				numVotes = curVotes;
				lowest = conNum;
			}
		}
		return lowest;
	}
	
	@Override
	public int findTopContestant() {
		int top = -1;
		int numVotes = -1;
		for(int conNum : votesByContestant.keySet()){
			int curVotes = votesByContestant.get(conNum).size();
			if(curVotes > numVotes) {
				numVotes = curVotes;
				top = conNum;
			}
		}
		return top;
	}
	
	@Override
	public boolean removeVotes(int contestant) {

		Set<Vote> callsForContestant = votesByContestant.get(contestant);
		for(Vote pc : callsForContestant) {
			votes.remove(pc.phoneNumber);
			totalNumVotes--;
		}
		return true;
	}

	@Override
	public boolean removeContestant(int contestant) {
		if(!votesByContestant.containsKey(contestant))
			return false;
		votesByContestant.remove(contestant);
		return true;
	}
	
	public String printStats(){
		String out = "Num Votes: " + totalNumVotes + "\n";
		out += "Current Leader: " + findTopContestant() + "\n";
		out += "Current Loser: " + findLowestContestant() + "\n";
		out += "Remaining Contestants: " + getNumRemainingContestants();
		return out;
	}

}
