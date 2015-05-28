
package edu.brown.benchmark.voteresper;

public abstract class VoterConstants {

    public static final String TABLENAME_CONTESTANTS     = "contestants";
    public static final String TABLENAME_AREA_CODE_STATE = "area_code_state";
    public static final String TABLENAME_VOTES           = "votes";
    
    public static int VOTE_THRESHOLD = 1000;
    public static final int BOARD_REFRESH = 100;
	public static final int MAX_VOTES = 1; 
	public static int NUM_CONTESTANTS = 12; 
	public static int INPUT_RATE = 1000;
	
	//public static String VOTE_DIR = "../data/";
	public static String VOTE_DIR = "/home/john/git/esper-voter/data/";
	public static String VOTE_FILE = "votes-XXX.txt";
	public static int NUM_THREADS = 1;
	public static int NUM_LINES = -1;
	public static int DURATION = 30;
	public static int QUEUE_SIZE = 10000;
	public static long SLEEP_TIME = 2;
	public static int WIN_SLIDE = 10;
	public static int WIN_SIZE = 100;
	public static boolean NO_ORDER = false;
	public static String OUT_FILE = "/home/john/git/esper-voter/data/out/out.txt";

	public static final String LOCAL_HOST = "localhost";

	// Initialize some common constants and variables
    public static final String CONTESTANT_NAMES_CSV = "Jann Arden,Micah Barnes,Justin Bieber,Jim Bryson,Michael Buble," +
    													"Leonard Cohen,Celine Dion,Nelly Furtado,Adam Gontier,Emily Haines," +
    													"Avril Lavigne,Ashley Leggat,Eileen McGann,Sarah McLachlan,Joni Mitchell," +
    													"Mae Moore,Alanis Morissette,Emilie Mover,Anne Murray,Sam Roberts," +
    													"Serena Ryder,Tamara Sandor,Nicholas Scribner,Shania Twain,Neil Young" + 
    													"Aann Jrden,Bicah Marnes,Bustin Jieber,Bim Jryson,Bichael Muble," +
    													"Ceonard Lohen,Deline Cion,Felly Nurtado,Gdam Aontier,Hmily Eaines," +
    													"Lvril Aavigne,Lshley Aeggat,Mileen EcGann,Marah ScLachlan,Moni Jitchell," +
    													"Nae Noore,Mlanis Aorissette,Mmilie Eover,Mnne Aurray,Ram Soberts," +
    													"Rerena Syder,Samara Tandor,Sicholas Ncribner,Thania Swain,Yeil Noung";
    
    public static final String VOTE_KEY = "Vote";
    public static final String LEADERBOARD_KEY = "Leaderboard";
    public static final String DELETE_KEY = "Delete";
    public static final String WORKFLOW_KEY = "Workflow";
    
    public static String getConfiguration(){
    	String s = "-------------------------------------------\n";
    	s += "RUNNING ESPER VOTER:\n";
    	s += "  Threads: " + NUM_THREADS + "\n";
    	s += "  Vote File: " + VOTE_DIR + VOTE_FILE + "\n";
    	s += "  Input Rate: " + INPUT_RATE + "\n";
    	s += "  Num Lines: " + NUM_LINES + "\n";
    	s += "  Duration: " + DURATION + "\n";
    	s += "  Num Contestants: " + NUM_CONTESTANTS + "\n";
    	s += "  Delete Threshold: " + VOTE_THRESHOLD + "\n";
    	s += "  No Order: " + NO_ORDER + "\n";
    	s += "  Out File: " + OUT_FILE + "\n";
    	s += "------------------------------";
    	return s;
    }
}
