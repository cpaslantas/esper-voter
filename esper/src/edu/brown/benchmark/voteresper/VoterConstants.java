
package edu.brown.benchmark.voteresper;

public abstract class VoterConstants {

    public static final String TABLENAME_CONTESTANTS     = "contestants";
    public static final String TABLENAME_AREA_CODE_STATE = "area_code_state";
    public static final String TABLENAME_VOTES           = "votes";
    
    public static final int VOTE_THRESHOLD = 1000;
    public static final int BOARD_REFRESH = 100;
	public static final int MAX_VOTES = 1; 
	public static final int NUM_CONTESTANTS = 12; 
	public static int INPUT_RATE = 1000;
	
	//public static String VOTE_DIR = "../data/";
	public static String VOTE_DIR = "/Users/john/git/esper-voter/data/";
	public static String VOTE_FILE = "votes-XXX.txt";
	public static int QUEUE_SIZE = 10000;
	public static long SLEEP_TIME = 2;

	public static final String LOCAL_HOST = "localhost";

	// Initialize some common constants and variables
    public static final String CONTESTANT_NAMES_CSV = "Jann Arden,Micah Barnes,Justin Bieber,Jim Bryson,Michael Buble," +
    													"Leonard Cohen,Celine Dion,Nelly Furtado,Adam Gontier,Emily Haines," +
    													"Avril Lavigne,Ashley Leggat,Eileen McGann,Sarah McLachlan,Joni Mitchell," +
    													"Mae Moore,Alanis Morissette,Emilie Mover,Anne Murray,Sam Roberts," +
    													"Serena Ryder,Tamara Sandor,Nicholas Scribner,Shania Twain,Neil Young";
    
    public static final String VOTE_KEY = "Vote";
    public static final String LEADERBOARD_KEY = "Leaderboard";
    public static final String DELETE_KEY = "Delete";
    public static final String WORKFLOW_KEY = "Workflow";
}
