/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.											   *
 *  Ported By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)  *                                                                      *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

package edu.brown.benchmark.voteresper;

public abstract class VoterConstants {

    public static final String TABLENAME_CONTESTANTS     = "contestants";
    public static final String TABLENAME_AREA_CODE_STATE = "area_code_state";
    public static final String TABLENAME_VOTES           = "votes";
    
    public static final int VOTE_THRESHOLD = 1000;
    public static final int BOARD_REFRESH = 100;
	public static final int MAX_VOTES = 1; 
	public static final int NUM_CONTESTANTS = 12; 
	
	public static final String VOTE_FILE = "/home/john/git/s-store/logs/votes-XXX.txt";
	public static final int DELETE_CODE = -1;
	public static final boolean SOCKET_CONTROL = false;
	
	public static final String HOST_PREFIX = "demo";
	public static final String HOST_PREFIX_2 = "Developmint";
	public static final String SERVER_HOST_NAME = "demo";
	public static final String JIANG_SERVER_HOST_NAME = "tutu";
	public static final String JIANG_SERVER_HOST_NAME_2 = "istc7";
	public static final String JIANG_HOST = "tutu";
	public static final String ISTC1_HOST = "istc1";
	public static final String ISTC1_CLIENT = "istc2";
	public static final int SERVER_PORT_NUM = 9510;
	public static final int VOTE_PORT_NUM = 9512;
	public static final String LOCAL_HOST = "localhost";

	// Initialize some common constants and variables
    public static final String CONTESTANT_NAMES_CSV = "Jann Arden,Micah Barnes,Justin Bieber,Jim Bryson,Michael Buble," +
    													"Leonard Cohen,Celine Dion,Nelly Furtado,Adam Gontier,Emily Haines," +
    													"Avril Lavigne,Ashley Leggat,Eileen McGann,Sarah McLachlan,Joni Mitchell," +
    													"Mae Moore,Alanis Morissette,Emilie Mover,Anne Murray,Sam Roberts," +
    													"Serena Ryder,Tamara Sandor,Nicholas Scribner,Shania Twain,Neil Young";
    
    
    // potential return codes
    public static final long STATUS_NOT_DETERMINED = -1;
    public static final long VOTE_SUCCESSFUL = 0;
    public static final long ERR_INVALID_CONTESTANT = 1;
    public static final long ERR_VOTER_OVER_VOTE_LIMIT = 2;
    public static final long ERR_NO_VOTE_FOUND = 3;
    public static final long DELETE_CONTESTANT = 4;
    public static final long WINDOW_SUCCESSFUL = 5;
    public static final long ERR_NOT_ENOUGH_CONTESTANTS = 6;
    public static final long DELETE_SUCCESSFUL = 7;
    public static final long BM_FINISHED = 8;
    
    public static final boolean DEBUG = false;
    public static final int WAIT_TIME = 2000;
}
