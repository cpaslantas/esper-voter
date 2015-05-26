package edu.brown.benchmark.voteresper;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class StatsCollector {
	private long startTime;
	private long duration;
	private HashMap<String, AtomicLong> totalLatency;
	private HashMap<String, AtomicLong> totalCount;
	private HashMap<String, AtomicLong> minLatency;
	private HashMap<String, AtomicLong> maxLatency;
	
	public StatsCollector() {
		totalLatency = new HashMap<String, AtomicLong>();
		totalCount = new HashMap<String, AtomicLong>();
		initializeStats();
	}
	
	public void initializeStats() {
		initializeValue("Vote");
		initializeValue("GenerateLeaderboard");
		initializeValue("DeleteContestant");
	}
	
	public void initializeValue(String value){
		totalLatency.put(value, new AtomicLong(0));
		totalCount.put(value, new AtomicLong(0));
		minLatency.put(value, new AtomicLong(Long.MAX_VALUE));
		maxLatency.put(value, new AtomicLong(0));
	}
	
	public void addStat(String stat, long duration) {
		totalLatency.get(stat).getAndAdd(duration);
		totalCount.get(stat).getAndIncrement();
		long min = minLatency.get(stat).get();
		if(duration < min)
			minLatency.get(stat).set(duration);
				
		long max = maxLatency.get(stat).get();
		if(duration > max)
			maxLatency.get(stat).set(duration);
	}

}
