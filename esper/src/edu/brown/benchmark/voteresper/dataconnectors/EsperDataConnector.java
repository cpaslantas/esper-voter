package edu.brown.benchmark.voteresper.dataconnectors;

import edu.brown.benchmark.voteresper.PhoneCall;
import edu.brown.benchmark.voteresper.Vote;

public abstract class EsperDataConnector {
	
	public abstract boolean hasVoted(long phoneNumber);
	public abstract boolean realContestant(int contestant);
	
	public abstract long numTimesVoted(long phoneNumber);
	public abstract String getState(long phoneNumber);
	public abstract long getNumVotes();
	public abstract int getAllVotesEver();
	public abstract long getNumRemainingContestants();
	
	public abstract boolean insertVote(Vote v);
	public abstract boolean updateLeaderboards(Vote v);
	public abstract int findLowestContestant();
	public abstract int findTopContestant();
	public abstract boolean removeContestant(int contestant);
	
	public abstract String printStats();
	
}
