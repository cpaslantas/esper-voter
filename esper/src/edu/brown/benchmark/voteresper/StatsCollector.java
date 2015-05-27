package edu.brown.benchmark.voteresper;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import edu.brown.benchmark.voteresper.VoterConstants;

public class StatsCollector {
	private long startTime;
	private long totalDuration;
	private HashMap<String, AtomicLong> totalLatency;
	private HashMap<String, AtomicLong> totalCount;
	private HashMap<String, AtomicLong> minLatency;
	private HashMap<String, AtomicLong> maxLatency;
	private HashMap<String, Double> avgLatency;
	private HashMap<String, Double> avgThroughput;
	
	public StatsCollector() {
		initializeStats();
	}
	
	public void initializeStats() {
		totalLatency = new HashMap<String, AtomicLong>();
		totalCount = new HashMap<String, AtomicLong>();
		minLatency = new HashMap<String, AtomicLong>();
		maxLatency = new HashMap<String, AtomicLong>();
		avgLatency = new HashMap<String, Double>();
		avgThroughput = new HashMap<String, Double>();
		initializeValue(VoterConstants.VOTE_KEY);
		initializeValue(VoterConstants.LEADERBOARD_KEY);
		initializeValue(VoterConstants.DELETE_KEY);
		initializeValue(VoterConstants.WORKFLOW_KEY);
	}
	
	public void initializeValue(String value){
		totalLatency.put(value, new AtomicLong(0));
		totalCount.put(value, new AtomicLong(0));
		minLatency.put(value, new AtomicLong(Long.MAX_VALUE));
		maxLatency.put(value, new AtomicLong(0));
		avgLatency.put(value, new Double(-1.0));
		avgThroughput.put(value, new Double(0.0));
	}
	
	public void closeWorkflow(long startTime, long endTime) {
		addStat("TotalWorkflow", startTime, endTime);
	}
	
	public void addStat(String stat, long startTime, long endTime) {
		long duration = endTime - startTime;
		totalLatency.get(stat).getAndAdd(duration);
		totalCount.get(stat).getAndIncrement();
		long min = minLatency.get(stat).get();
		if(duration < min)
			minLatency.get(stat).set(duration);
				
		long max = maxLatency.get(stat).get();
		if(duration > max)
			maxLatency.get(stat).set(duration);
	}
	
	public void updateTotals() {
		long endTime = System.nanoTime();
		totalDuration = endTime - startTime;
		double durSec = EPRuntimeUtil.nanoToSeconds(endTime - startTime);
		for(String key : totalLatency.keySet()) {
			double totalLat = (double)totalLatency.get(key).get();
			double totalCnt = (double)totalCount.get(key).get();
			avgLatency.put(key, totalLat / totalCnt);
			avgThroughput.put(key, totalCnt/durSec);
		}
	}
	
	public void printStats() {
		updateTotals();
		System.out.println("STATS AS OF " + EPRuntimeUtil.nanoToSeconds(totalDuration) + " SECONDS:");
		for(String key : totalLatency.keySet()) {
			double avgLat = EPRuntimeUtil.nanoToSeconds(avgLatency.get(key));
			double minLat = EPRuntimeUtil.nanoToSeconds(minLatency.get(key).get());
			double maxLat = EPRuntimeUtil.nanoToSeconds(maxLatency.get(key).get());
			double avgThput = avgThroughput.get(key);
			System.out.println(key + ": AVG THPUT: " + avgThput + " AVG LAT: " + avgLat + " MIN LAT: " + minLat + " MAX LAT: " + maxLat);
			System.out.println("TUPLE COUNT: " + totalCount.get(key).get());
		}
		System.out.println("---------------------------");
	}

}
