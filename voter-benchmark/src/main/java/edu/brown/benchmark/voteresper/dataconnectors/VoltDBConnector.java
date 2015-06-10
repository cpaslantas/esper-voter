package edu.brown.benchmark.voteresper.dataconnectors;

import org.voltdb.jdbc.Driver;

import com.espertech.esper.client.EPOnDemandQueryResult;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventBean;

import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.tuples.Vote;

import java.sql.*;

/**
 * Created by cpa on 5/19/15.
 */
public class VoltDBConnector extends EsperDataConnector{
    Connection dbconn;
    private static long lastVoteId = 0;
    private EPServiceProvider cep;
	private EPRuntime cepRT;
	private int numContestants;
	private int allVotesEver;
	private long cutoffVote;
    
    public VoltDBConnector(int numContestants, EPServiceProvider cep, StatsCollector stats) {
    	super(stats);
    	this.numContestants = numContestants;
    	this.cep = cep;
    	this.cepRT = cep.getEPRuntime();
    	this.allVotesEver = 0;
    	this.cutoffVote = 0;
        dbconn = getConnection();
        if(dbconn == null) {
            System.err.println("JDBC Connection Error: Connection failed.");
        }
        initializeDatabase();
        populateDatabase(numContestants);
    }
    
    public Connection getConnection() {
        Connection conn = null;
        Driver driver = null;
        
        try {
            Class.forName("org.voltdb.jdbc.Driver" );

            conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");
            //conn.setNetworkTimeout(null, 60000);
            conn.setAutoCommit(true);
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return conn;
        
    }
    
    public void dropTables() {
    	String vdl = "DROP VIEW v_votes_by_phone_number IF EXISTS;";
    	String vcdl = "DROP VIEW v_votes_by_contestant_number IF EXISTS;";
    	String cdl = "DROP TABLE contestants IF EXISTS;";
    	String ddl = "DROP TABLE votes IF EXISTS;";
    	
    	executeQuery(vdl);
    	executeQuery(vcdl);
    	executeQuery(cdl);
    	executeQuery(ddl);
    }
    
    public void initializeDatabase() {
        String cdl = "CREATE TABLE contestants\n" +
                        "(\n" +
                        "  contestant_number integer     NOT NULL\n" +
                        ",  contestant_name varchar(50)   NOT NULL\n" +
                        ", CONSTRAINT PK_contestants PRIMARY KEY\n" +
                        "  (\n" +
                        "    contestant_number\n" +
                        "  )\n" +
                        ");";
        
        // AUTOINCREMENT ?
        String ddl =  "CREATE TABLE votes\n" +
                        "(\n" +
                        "  vote_id            bigint     NOT NULL,\n" +
                        "  phone_number       bigint     NOT NULL\n" +
                        ", contestant_number  integer    NOT NULL REFERENCES contestants (contestant_number)\n" +
                        ", created            timestamp  NOT NULL" +
                        ", CONSTRAINT PK_votes PRIMARY KEY\n" +
                        "  (\n" +
                        "    vote_id\n" +
                        "  )\n" +
                        ");";
        
        String vdl = "CREATE VIEW v_votes_by_phone_number\n" +
                "(\n" +
                "  phone_number\n" +
                ", num_votes\n" +
                ")\n" +
                "AS\n" +
                "   SELECT phone_number\n" +
                "        , COUNT(*)\n" +
                "     FROM votes\n" +
                " GROUP BY phone_number\n" +
                ";";
        
        String vcdl = "CREATE VIEW v_votes_by_contestant_number\n" +
                "(\n" +
                "  contestant_number\n" +
                ", num_votes\n" +
                ")\n" +
                "AS\n" +
                "   SELECT contestant_number\n" +
                "        , COUNT(*)\n" +
                "     FROM votes\n" +
                " GROUP BY contestant_number\n" +
                ";";

        dropTables();
        executeQuery(cdl);
        executeQuery(ddl);
        executeQuery(vdl);
        executeQuery(vcdl);
        
    }
    
    private void populateDatabase(int numContestants) {
		for(int i = 0; i < numContestants; i++) {
			insertContestant(i+1, CONTESTANT_NAMES[i]);
		}
//		assert areaCodes.length == states.length;
//		for(int i = 0; i < areaCodes.length; i++) {
//			cepRT.executeQuery("insert into area_code_state values (" + areaCodes[i] + ",'" + states[i] + "')");
//		}
	}
    
    public void executeQuery(String query) {
        try {
            Statement createContestantStmt = dbconn.createStatement();
            createContestantStmt.execute(query);

//            SQLWarning warn = createContestantStmt.getWarnings();
//            if (warn != null)
//                System.out.println("warn: " + warn.getMessage());

        } catch (SQLException e) {
            e.printStackTrace();
        }
        
    }
    
    public void insertContestant(int id, String name) {
        final String idl = "INSERT INTO contestants (contestant_number, contestant_name) VALUES (" + id + ", '" + name + "');";
    
        try {
            Statement stmt = dbconn.createStatement();
            stmt.execute(idl);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public void insertVote(long phoneNumber, int contestant){
        String idl = "INSERT INTO votes (vote_id, phone_number, contestant_number, created) VALUES (?, ?, ?, ?, ?);";
        String[] values = {Long.toString(lastVoteId), Long.toString(phoneNumber), Integer.toString(contestant), new Timestamp(System.currentTimeMillis()).toString() };
        
        try {
            Statement stmt = dbconn.createStatement();
            stmt.executeUpdate(idl, values);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public boolean isContestant(int id) {
        String cdl = "SELECT contestant_number FROM contestants WHERE contestant_number = " + id + ";";

        try {
            Statement stmt = dbconn.createStatement();
            ResultSet rs = stmt.executeQuery(cdl);
            if (!rs.next()){
                return false;
            } else {
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return false;
    }

    public boolean voteExists(long phoneNumber) {
        String cdl = "SELECT * FROM votes WHERE phone_number = " + phoneNumber + ";";

        try {
            Statement stmt = dbconn.createStatement();
            ResultSet rs = stmt.executeQuery(cdl);
            if (!rs.next()){
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }
    
//    public void removeContestant(int id) {
//        String vdl = "DELETE FROM votes WHERE contestant_number = ?;";
//        String cdl = "DELETE FROM contestants WHERE contestant_number = ?;";
//
//        try {
//            Statement stmt = dbconn.createStatement();
//            stmt.executeUpdate(vdl, id);
//            stmt.executeUpdate(cdl, id);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
    
    @Override
	public boolean hasVoted(long phoneNumber) {
		return numTimesVoted(phoneNumber) > 0;
	}

	@Override
	public boolean realContestant(int contestant) {
		EPOnDemandQueryResult result = cepRT.executeQuery("select contestant_number from contestants where contestant_number = " + contestant); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return false;
		else
			return true;
	}

	@Override
	public long numTimesVoted(long phoneNumber) {
		EPOnDemandQueryResult result = cepRT.executeQuery("select count(*) as num_votes from votes_tbl where phone_number = " + phoneNumber); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return -1;
		
		return (Long)e[0].get("num_votes");
	}

	@Override
	public String getState(long phoneNumber) {
		short areaCode = (short)(phoneNumber/10000000l);
		EPOnDemandQueryResult result = cepRT.executeQuery("select state from area_code_state where area_code = " + areaCode); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return "XX";
		
		return (String)e[0].get("state");
	}
	
	public long getNumRemainingContestants() {
		EPOnDemandQueryResult result = cepRT.executeQuery("select count(*) as num_contestants from contestants"); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return -1;
		
		return (Long)e[0].get("num_contestants");
	}

	@Override
	public long getNumVotes() {
		EPOnDemandQueryResult result = cepRT.executeQuery("select count(*) as num_votes from votes_tbl"); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return -1;
		
		return (Long)e[0].get("num_votes");
	}
	
	@Override
	public int getAllVotesEver() {
		return allVotesEver;
	}

	@Override
	public boolean insertVote(Vote v) {
		cepRT.executeQuery("insert into votes_tbl values (" + v.outputValues() + ")"); 					
		allVotesEver++;
		return true;
	}
	
	@Override
	public long getLeaderboardSize() {
		EPOnDemandQueryResult result = cepRT.executeQuery("select count(*) as num_votes from leaderboard_tbl"); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return 0;
		
		return (Long)e[0].get("num_votes");
	}
	
	@Override
	public long getCutoffVote() {
		return cutoffVote;
	}
	
	@Override
	public void setCutoffVote(long cutoff) {
		cutoffVote = cutoff;
	}
	
	@Override
	public boolean deleteCutoff(long cutoff) {
		cepRT.executeQuery("delete from leaderboard_tbl where vote_id <= " + cutoff);
		return true;
	}

	@Override
	public boolean insertLeaderboard(Vote v) {
		cepRT.executeQuery("insert into leaderboard_tbl values (" + v.outputValues() + ")");
		return true;
	}

	@Override
	public int findLowestContestant() {
		EPOnDemandQueryResult result = cepRT.executeQuery("select contestant_number, count(*) as num_votes from votes_tbl group by contestant_number order by num_votes, contestant_number desc"); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return -1;
		
		return (Integer)e[0].get("contestant_number");
	}
	
	@Override
	public int findTopContestant() {
		EPOnDemandQueryResult result = cepRT.executeQuery("select contestant_number, count(*) as num_votes from votes_tbl group by contestant_number order by num_votes desc, contestant_number asc"); 
		EventBean[] e = result.getArray();
		if(e.length == 0)
			return -1;
		
		return (Integer)e[0].get("contestant_number");
	}
	
	@Override
	public boolean removeVotes(int contestant) {
		cepRT.executeQuery("delete from votes_tbl where contestant_number = " + contestant);
		return true;
	}

	@Override
	public boolean removeContestant(int contestant) {
		//System.out.println(printAllVotes());
		System.out.println("REMOVING CONTESTANT " + contestant);
		//cepRT.executeQuery("delete from votes_tbl where contestant_number = " + contestant);
		cepRT.executeQuery("delete from contestants where contestant_number = " + contestant);
		numContestants--;
		return true;
	}
	
	public String printAllVotes() {
		EPOnDemandQueryResult result = cepRT.executeQuery("select contestant_number, count(*) as num_votes from votes_tbl group by contestant_number order by num_votes, contestant_number desc"); 
		EventBean[] e = result.getArray();
		
		if(e.length == 0)
			return "";
		
		String o = "ALL VOTES: " + allVotesEver + "\n";
		o += "CUR VOTES: " + getNumVotes() + "\n";
		for(int i = 0; i < e.length; i++) {
			o += e[i].get("contestant_number") + "," + e[i].get("num_votes") + "\n";
		}
		o += "-----------\n";
		return o;		
	}
	
	public String printStats(){
		String out = "Total Num Votes: " + allVotesEver + "\n";
		out += "Current Leader: " + findTopContestant() + "\n";
		out += "Current Loser: " + findLowestContestant() + "\n";
		out += "Remaining Contestants: " + getNumRemainingContestants();
		return out;
	}
}
