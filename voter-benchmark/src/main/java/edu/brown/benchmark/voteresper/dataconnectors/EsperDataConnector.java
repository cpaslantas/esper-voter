package edu.brown.benchmark.voteresper.dataconnectors;

import java.util.concurrent.atomic.AtomicInteger;

import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.tuples.EsperTuple;
import edu.brown.benchmark.voteresper.tuples.PhoneCall;
import edu.brown.benchmark.voteresper.tuples.Vote;

public abstract class EsperDataConnector {
	
	public StatsCollector stats;
	public AtomicInteger completedWorkflows;
	
	public abstract boolean hasVoted(long phoneNumber);
	public abstract boolean realContestant(int contestant);
	
	public abstract long numTimesVoted(long phoneNumber);
	public abstract String getState(long phoneNumber);
	public abstract long getNumVotes();
	public abstract int getAllVotesEver();
	public abstract long getNumRemainingContestants();
	
	public abstract boolean insertVote(Vote v);
	
	public abstract long getLeaderboardSize();
	public abstract long getCutoffVote();
	public abstract void setCutoffVote(long cutoff);
	public abstract boolean deleteCutoff(long cutoff);
	public abstract boolean insertLeaderboard(Vote v);
	
	
	public abstract int findLowestContestant();
	public abstract int findTopContestant();
	public abstract boolean removeVotes(int contestant);
	public abstract boolean removeContestant(int contestant);
	
	public abstract String printStats();
	
	public EsperDataConnector(StatsCollector s) {
		stats = s;
		completedWorkflows = new AtomicInteger(0);
	}
	
	public void closeWorkflow(long startTime, long endTime) {
		completedWorkflows.getAndIncrement();
		stats.addStat(VoterConstants.WORKFLOW_KEY, startTime, endTime);
	}
	
	public void closeWorkflow(EsperTuple et) {
		et.end();
		completedWorkflows.getAndIncrement();
		stats.addStat(VoterConstants.WORKFLOW_KEY, et.tupleStartTime, et.endTime);
	}
	
	public int getCompletedWorkflows() {
		return completedWorkflows.get();
	}
	
}
