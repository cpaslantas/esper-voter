
package edu.brown.benchmark.voteresper;

public abstract class VoterConstants {

    public static final String TABLENAME_CONTESTANTS     = "contestants";
    public static final String TABLENAME_AREA_CODE_STATE = "area_code_state";
    public static final String TABLENAME_VOTES           = "votes";
    
    public static int VOTE_THRESHOLD = 20000;
    public static final int BOARD_REFRESH = 100;
	public static final int MAX_VOTES = 1; 
	public static int NUM_CONTESTANTS = 50; 
	public static int INPUT_RATE = 8000;
	
	//public static String VOTE_DIR = "../data/";
	public static String ROOT_DIR = "/home/john/git/esper-voter/";
	public static String VOTE_DIR_SUFFIX = "data/";
	public static String BENCHMARK_DIR_SUFFIX = "voter-benchmark/";
	public static String DDL_DIR_SUFFIX = BENCHMARK_DIR_SUFFIX + "src/main/java/edu/brown/benchmark/voteresper/voltsp/";
	public static String VOTE_DIR = ROOT_DIR + VOTE_DIR_SUFFIX;
	public static String BENCHMARK_DIR = ROOT_DIR + BENCHMARK_DIR_SUFFIX;
	public static String DDL_DIR = ROOT_DIR + DDL_DIR_SUFFIX;
	public static String DDL_FILE = "voter-voltdb.sql";
	public static String LOAD_DB_FILE = "reloadDB.sh"; 
	public static String VOTE_FILE = "votes-50-20000_1.txt";
	public static int NUM_THREADS = 1;
	public static int NUM_LINES = -1;
	public static int DURATION = 30000;
	public static int WARMUP_DURATION = 10000;
	//public static int QUEUE_SIZE = 10000;
	public static long SLEEP_TIME = 2;
	public static int WIN_SLIDE = 10;
	public static int WIN_SIZE = 100;
	public static boolean NO_ORDER = false;
	public static String ORDER = "false";
	public static String OUT_FILE = "/home/john/git/esper-voter/data/out/out.txt";
	public static boolean PRINT_TO_CONSOLE = false;
	public static boolean MEASURE_INPUT_ONLY = false;
	public static String BACKEND = "default";
	public static String LOG = "true";

	public static final String LOCAL_HOST = "localhost";

	// Initialize some common constants and variables
    public static final String CONTESTANT_NAMES_CSV = "Jann Arden,Micah Barnes,Justin Bieber,Jim Bryson,Michael Buble," +
    													"Leonard Cohen,Celine Dion,Nelly Furtado,Adam Gontier,Emily Haines," +
    													"Avril Lavigne,Ashley Leggat,Eileen McGann,Sarah McLachlan,Joni Mitchell," +
    													"Mae Moore,Alanis Morissette,Emilie Mover,Anne Murray,Sam Roberts," +
    													"Serena Ryder,Tamara Sandor,Nicholas Scribner,Shania Twain,Neil Young," + 
    													"Aann Jrden,Bicah Marnes,Bustin Jieber,Bim Jryson,Bichael Muble," +
    													"Ceonard Lohen,Deline Cion,Felly Nurtado,Gdam Aontier,Hmily Eaines," +
    													"Lvril Aavigne,Lshley Aeggat,Mileen EcGann,Marah ScLachlan,Moni Jitchell," +
    													"Nae Noore,Mlanis Aorissette,Mmilie Eover,Mnne Aurray,Ram Soberts," +
    													"Rerena Syder,Samara Tandor,Sicholas Ncribner,Thania Swain,Yeil Noung";
    
    public static final String VOTE_KEY = "Vote";
    public static final String LEADERBOARD_KEY = "Leaderboard";
    public static final String DELETE_KEY = "Delete";
    public static final String WORKFLOW_KEY = "Workflow";
    public static final String DUMMY_KEY = "Dummy";
    
    public static final String DUMMY_BACKEND = "dummy";
    public static final String ESPER_BACKEND = "esper";
    public static final String VOLTDB_BACKEND = "voltdb";
    public static final String VOLTDBADHOC_BACKEND = "voltdbadhoc";
    
    public static final String VOTER_TYPE = "Voter";
    
    public static final String COMMAND_LOG = "EsperVoterDurableLog";
    
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
    	s += "  Order: " + ORDER + "\n";
    	s += "  Out File: " + OUT_FILE + "\n";
    	s += "  Backend: " + BACKEND + "\n";
    	s += "  Log: " + LOG + "\n";
    	s += "------------------------------";
    	return s;
    }
    
    public static void changeRootDir(String newRoot) {
    	ROOT_DIR = newRoot;
    	VOTE_DIR = newRoot + VOTE_DIR_SUFFIX;
    	BENCHMARK_DIR = newRoot + BENCHMARK_DIR_SUFFIX;
    	DDL_DIR = newRoot + DDL_DIR_SUFFIX;
    }
}
