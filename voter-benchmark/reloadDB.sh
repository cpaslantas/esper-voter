#!/bin/bash
rootdir=$1
sqldir="${rootdir}/git/voltdb/bin"
filedir="${rootdir}/git/esper-voter/voter-benchmark/src/main/java/edu/brown/benchmark/voteresper/voltsp"

${sqldir}/sqlcmd <<EOD
load classes ${filedir}/storedprocs.jar;
EOD

${sqldir}/sqlcmd <<EOD
file ${filedir}/voter-voltdb.sql;
EOD