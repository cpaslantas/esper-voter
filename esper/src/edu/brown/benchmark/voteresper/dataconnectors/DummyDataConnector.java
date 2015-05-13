package edu.brown.benchmark.voteresper.dataconnectors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.brown.benchmark.voteresper.PhoneCall;

public class DummyDataConnector extends EsperDataConnector {
	
	private HashMap<Long, PhoneCall> votes;
	private HashMap<Integer, Set<PhoneCall>> votesByContestant;
	private int numContestants;
	private int totalNumVotes;
	
	public DummyDataConnector(int numContestants){
		this.numContestants = numContestants;
		votes = new HashMap<Long, PhoneCall>();
		votesByContestant = new HashMap<Integer, Set<PhoneCall>>();
		for(int i = 0; i < numContestants; i++) {
			votesByContestant.put(i+1, new HashSet<PhoneCall>());
		}
		totalNumVotes = 0;
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
	public int numTimesVoted(long phoneNumber) {
		if(votes.containsKey(phoneNumber))
			return 1;
		else
			return 0;
	}

	@Override
	public String getState(long phoneNumber) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getNumVotes() {
		return totalNumVotes;
	}

	@Override
	public boolean insertVote(PhoneCall pc) {
		if(votes.containsKey(pc.phoneNumber))
			return false;
		
		int contestantId = pc.contestantNumber;
		votes.put(pc.phoneNumber, pc);
		votesByContestant.get(contestantId).add(pc);
		totalNumVotes++;
		return true;
	}

	@Override
	public boolean updateLeaderboards(PhoneCall pc) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int findLowestContestant() {
		int lowest = -1;
		int numVotes = -1;
		for(int i = 1; i <= numContestants; i++){
			if(!votesByContestant.containsKey(i))
				continue;
			int curVotes = votesByContestant.get(i).size();
			if(curVotes < numVotes) {
				numVotes = curVotes;
				lowest = i;
			}
		}
		return lowest;
	}

	@Override
	public boolean removeContestant(int contestant) {
		if(!votesByContestant.containsKey(contestant))
			return false;
		
		Set<PhoneCall> callsForContestant = votesByContestant.get(contestant);
		for(PhoneCall pc : callsForContestant) {
			votes.remove(pc.phoneNumber);
			totalNumVotes--;
		}
		votesByContestant.remove(contestant);
		return true;
	}

}
