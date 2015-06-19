package edu.brown.benchmark.voteresper.dataconnectors;

import org.voltdb.jdbc.Driver;

import com.espertech.esper.client.EPOnDemandQueryResult;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventBean;

import edu.brown.benchmark.voteresper.EPRuntimeUtil;
import edu.brown.benchmark.voteresper.StatsCollector;
import edu.brown.benchmark.voteresper.VoterConstants;
import edu.brown.benchmark.voteresper.tuples.PhoneCall;
import edu.brown.benchmark.voteresper.tuples.ToDelete;
import edu.brown.benchmark.voteresper.tuples.Vote;

import java.sql.*;

/**
 * Created by cpa on 5/19/15.
 */
public class VoltDBSPConnector extends EsperDataConnector{
    Connection dbconn;
    private static long lastVoteId = 0;
    private EPServiceProvider cep;
	private EPRuntime cepRT;
	private int numContestants;
	private int allVotesEver;
	private long cutoffVote;
    
    public VoltDBSPConnector(int numContestants, EPServiceProvider cep, StatsCollector stats) {
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
        //initializeDatabase();
        //dbconn.getMetaData();
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
    	String acsdl = "DROP TABLE area_code_state IF EXISTS;";
    	String lbtdl = "DROP TABLE leaderboard_tbl IF EXISTS;";
    	String cdl = "DROP TABLE contestants_tbl IF EXISTS;";
    	String ddl = "DROP TABLE votes_tbl IF EXISTS;";
    	
    	executeQuery(vdl);
    	executeQuery(vcdl);
    	executeQuery(cdl);
    	executeQuery(ddl);
    	executeQuery(acsdl);
    	executeQuery(lbtdl);
    }
    
    public void initializeDatabase() {
    	/**
        String cdl = "CREATE TABLE contestants_tbl\n" +
                        "(\n" +
                        "  contestant_number integer     NOT NULL\n" +
                        ",  contestant_name varchar(50)   NOT NULL\n" +
                        ", CONSTRAINT PK_contestants PRIMARY KEY\n" +
                        "  (\n" +
                        "    contestant_number\n" +
                        "  )\n" +
                        ");";
        
        // AUTOINCREMENT ?
        String ddl =  "CREATE TABLE votes_tbl\n" +
                        "(\n" +
                        "  vote_id            bigint     NOT NULL\n" +
                        ", contestant_number  integer    NOT NULL REFERENCES contestants_tbl (contestant_number)\n" +
                        ", phone_number       bigint     NOT NULL\n" +
                        ", state              varchar(2) NOT NULL\n" +
                        ", created            timestamp  NOT NULL" +
                        ", CONSTRAINT PK_votes PRIMARY KEY\n" +
                        "  (\n" +
                        "    vote_id\n" +
                        "  )\n" +
                        ");";
        
        String acs = "create table area_code_state (" +
				 "area_code int primary key, " +
				 "state varchar(2) NOT NULL);";
        
		String lbt = "create table leaderboard_tbl (vote_id bigint primary key, " +
				 " contestant_number  int NOT NULL, " +
				 "phone_number bigint NOT NULL, " + 
				 " state varchar(2) NOT NULL, " +
				 "created	     bigint NOT NULL)";
        
        String vdl = "CREATE VIEW v_votes_by_phone_number\n" +
                "(\n" +
                "  phone_number\n" +
                ", num_votes\n" +
                ")\n" +
                "AS\n" +
                "   SELECT phone_number\n" +
                "        , COUNT(*)\n" +
                "     FROM votes_tbl\n" +
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
                "     FROM votes_tbl\n" +
                " GROUP BY contestant_number\n" +
                ";";

        dropTables();
        executeQuery(cdl);
        executeQuery(ddl);
        executeQuery(acs);
        executeQuery(lbt);
        executeQuery(vdl);
        executeQuery(vcdl);
        */
        
    }
    
    private void populateDatabase(int numContestants) {
    	/**
		for(int i = 0; i < numContestants; i++) {
			insertContestant(i+1, CONTESTANT_NAMES[i]);
		}
		assert areaCodes.length == states.length;
		for(int i = 0; i < areaCodes.length; i++) {
			executeQuery("insert into area_code_state values (" + areaCodes[i] + ",'" + states[i] + "')");
		}*/
    	runInitialize(numContestants);
	}
    
    public boolean executeQuery(String query) {
        try {
            Statement createContestantStmt = dbconn.createStatement();
            return createContestantStmt.execute(query);

//            SQLWarning warn = createContestantStmt.getWarnings();
//            if (warn != null)
//                System.out.println("warn: " + warn.getMessage());

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }
    
    public ResultSet executeQueryWithResult(String query) {
        try {
            Statement stmt = dbconn.createStatement();
            return stmt.executeQuery(query);

//            SQLWarning warn = createContestantStmt.getWarnings();
//            if (warn != null)
//                System.out.println("warn: " + warn.getMessage());

        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public void insertContestant(int id, String name) {
        final String idl = "INSERT INTO contestants_tbl (contestant_number, contestant_name) VALUES (" + id + ", '" + name + "');";
    
        try {
            Statement stmt = dbconn.createStatement();
            stmt.execute(idl);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    @Override
	public boolean hasVoted(long phoneNumber) {
		return numTimesVoted(phoneNumber) > 0;
	}

	@Override
	public boolean realContestant(int contestant) {
		try {
			ResultSet result = executeQueryWithResult("select contestant_number from contestants_tbl where contestant_number = " + contestant);
			
			if(result == null || result.first() == false)
				return false;
			else
				return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public long numTimesVoted(long phoneNumber) {
		try {
			ResultSet result = executeQueryWithResult("select count(*) as num_votes from votes_tbl where phone_number = " + phoneNumber);
			
			if(result == null || result.first() == false)
				return -1;
			else
				return result.getLong("num_votes");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}

	@Override
	public String getState(long phoneNumber) {
		try {
			short areaCode = (short)(phoneNumber/10000000l);
			ResultSet result = executeQueryWithResult("select state from area_code_state where area_code = " + areaCode);
			
			if(result == null || result.first() == false)
				return "XX";
			else
				return result.getString("state");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "XX";
		}
	}
	
	public long getNumRemainingContestants() {
		try {
			ResultSet result = executeQueryWithResult("select count(*) as num_contestants from contestants_tbl");
			
			if(result == null || result.first() == false)
				return -1;
			else
				return result.getLong("num_contestants");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}

	@Override
	public long getNumVotes() {
		try {
			ResultSet result = executeQueryWithResult("select count(*) as num_votes from votes_tbl");
			
			if(result == null || result.first() == false)
				return -1;
			else
				return result.getLong("num_votes");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}
	
	@Override
	public int getAllVotesEver() {
		return allVotesEver;
	}

	@Override
	public boolean insertVote(Vote v) {
		boolean success = executeQuery("insert into votes_tbl (" + Vote.outputColumns() + " ) values (" + v.outputValues() + ")");
		if(success)
			allVotesEver++;
		
		return success;
	}
	
	@Override
	public long getLeaderboardSize() {
		try {
			ResultSet result = executeQueryWithResult("select count(*) as num_votes from leaderboard_tbl");
			
			if(result == null || result.first() == false)
				return -1;
			else
				return result.getLong("num_votes");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
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
		return executeQuery("delete from leaderboard_tbl where vote_id <= " + cutoff);
	}

	@Override
	public boolean insertLeaderboard(Vote v) {
		return executeQuery("insert into leaderboard_tbl values (" + v.outputValues() + ")");
	}

	@Override
	public int findLowestContestant() {
		try {
			ResultSet result = executeQueryWithResult("select contestant_number, num_votes from v_votes_by_contestant_number order by num_votes, contestant_number desc");
			
			if(result == null || result.first() == false)
				return -1;
			else
				return result.getInt("contestant_number");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}
	
	@Override
	public int findTopContestant() {
		try {
			ResultSet result = executeQueryWithResult("select contestant_number, num_votes from v_votes_by_contestant_number order by num_votes desc, contestant_number asc");
			
			if(result == null || result.first() == false)
				return -1;
			else
				return result.getInt("contestant_number");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}
	
	@Override
	public boolean removeVotes(int contestant) {
		return executeQuery("delete from votes_tbl where contestant_number = " + contestant);
	}

	@Override
	public boolean removeContestant(int contestant) {
		//System.out.println(printAllVotes());
		System.out.println("REMOVING CONTESTANT " + contestant);
		boolean result = executeQuery("delete from contestants_tbl where contestant_number = " + contestant);
		if(result)
			numContestants--;
		return result;
	}
	
	public String printAllVotes() {
		try {
			ResultSet result = executeQueryWithResult("select contestant_number, count(*) as num_votes from v_votes_by_contestant_number order by num_votes, contestant_number desc"); 
			
			if(result == null || result.first() == false)
				return "NO VOTES FOUND\n";
			
			String o = "ALL VOTES: " + allVotesEver + "\n";
			o += "CUR VOTES: " + getNumVotes() + "\n";
			
			do {
				o += result.getInt("contestant_number") + "," + result.getInt("num_votes") + "\n";
			}
			while(result.next());
			
			o += "-----------\n";
			return o;		

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "ERROR: Could not print votes\n";
		}
	}
	
	public String printStats(){
		String out = "Total Num Votes: " + allVotesEver + "\n";
		out += "Current Leader: " + findTopContestant() + "\n";
		out += "Current Loser: " + findLowestContestant() + "\n";
		out += "Remaining Contestants: " + getNumRemainingContestants();
		return out;
	}
	
	public void runInitialize(int numContestants) {
		try {
			CallableStatement stmt = dbconn.prepareCall("{call InitializeSP(?,?)}");
			stmt.setLong(1, numContestants);
			stmt.setString(2, VoterConstants.CONTESTANT_NAMES_CSV);
			ResultSet result = stmt.executeQuery();
			
		}
		catch(SQLException e) {
			System.err.println(e.getMessage());
		}
	}
	
	public Vote runSP1(PhoneCall pc) {
		try {
			CallableStatement stmt = dbconn.prepareCall("{call VoteSP(?,?,?,?)}");
			stmt.setLong(1, pc.voteId);
			stmt.setLong(2, pc.phoneNumber);
			stmt.setInt(3, pc.contestantNumber);
			stmt.setInt(4, VoterConstants.MAX_VOTES);
			ResultSet result = stmt.executeQuery();
			
			if(result == null || result.first() == false)
				return null;
			else {
				
				return new Vote(result.getLong("vote_id"), result.getInt("contestant_number"), result.getLong("phone_number"), result.getString("state"), System.nanoTime(), pc.getInTime() );
				//return null;
			}
		}
		catch(SQLException e) {
			System.err.println(e.getMessage());
			return null;
		}
	}
	
	public ToDelete runSP2(Vote v) {
		try {
			CallableStatement stmt = dbconn.prepareCall("{call GenerateLeaderboardSP(?,?,?,?,?)}");
			stmt.setLong(1, v.voteId);
			stmt.setLong(2, v.phoneNumber);
			stmt.setString(3, v.state);
			stmt.setLong(4, v.contestantNumber);
			stmt.setLong(5, v.startTime);
			ResultSet result = stmt.executeQuery();
			
			if(result == null || result.first() == false)
				return null;
			else {
				
				return new ToDelete(result.getInt("contestant_number"), System.nanoTime(), v.getTupleStartTime() );
				//return null;
			}
		}
		catch(SQLException e) {
			System.err.println(e.getMessage());
			return null;
		}
	}
	
	public boolean runSP3(ToDelete td) {
		try {
			CallableStatement stmt = dbconn.prepareCall("{call DeleteContestantSP(?)}");
			stmt.setLong(1, td.contestantNumber);
			ResultSet result = stmt.executeQuery();
			
			if(result == null || result.first() == false)
				return false;
			else {
				return true;
			}
		}
		catch(SQLException e) {
			System.err.println(e.getMessage());
			return false;
		}
	}
}
