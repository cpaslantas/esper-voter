#/bin/bash

curdir=$(pwd)
spdir="${curdir}/src/main/java/edu/brown/benchmark/voteresper/voltsp/"

cd "${spdir}"
javac -cp "$CLASSPATH:${curdir}/lib/*" VoteSP.java
javac -cp "$CLASSPATH:${curdir}/lib/*" GenerateLeaderboardSP.java
javac -cp "$CLASSPATH:${curdir}/lib/*" DeleteContestantSP.java

jar cvf storedprocs.jar *.class

#voltdir="/home/john/git/voltdb/bin/"
#${voltdir}sqlcmd
#file /home/john/git/esper-voter/voter-benchmark/src/main/java/edu/brown/benchmark/voteresper/voltsp/voter-voltdb.sql;
