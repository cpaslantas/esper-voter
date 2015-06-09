package edu.brown.benchmark.voteresper.tuples;

public abstract class EsperTuple {
	public long tupleStartTime;
    public long startTime;
    public long endTime;
    public final String tupleType;
    
    public EsperTuple(String type) {
    	tupleType = type;
    	startTime = System.nanoTime();
    }
    
    public void end() {
    	endTime = System.nanoTime();
    }
}
