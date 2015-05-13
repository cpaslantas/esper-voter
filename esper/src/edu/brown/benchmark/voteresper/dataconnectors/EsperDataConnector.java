package edu.brown.benchmark.voteresper.dataconnectors;

import edu.brown.benchmark.voteresper.PhoneCall;

public abstract class EsperDataConnector {
	
	public abstract boolean hasVoted(long phoneNumber);
	public abstract boolean realContestant(int contestant);
	
	public abstract int numTimesVoted(long phoneNumber);
	public abstract String getState(long phoneNumber);
	public abstract int getNumVotes();
	
	public abstract boolean insertVote(PhoneCall pc);
	public abstract boolean updateLeaderboards(PhoneCall pc);
	public abstract int findLowestContestant();
	public abstract boolean removeContestant(int contestant);
	
}
