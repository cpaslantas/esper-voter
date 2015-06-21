#!/bin/bash

trap "exit" INT
INPUT=200
FINAL=3000
DIR="jlmeehan"
VOLTDB="/home/${DIR}/git/voltdb/bin/voltdb create --deployment=/home/${DIR}/git/esper-voter/voter-benchmark/voltdbconfig.xml --host=localhost --http=8083"
#rm "/home/${DIR}/git/esper-voter/data/out/out.txt"

while [ "${INPUT}" -lt "${FINAL}" ];
do
	ant run -Dthread=1 -Ddir="/home/${DIR}/git/esper-voter/" -Dfile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/0621/esper-ramp-1.txt" -Drate="1x${INPUT}" -Dcontestants=50 \
		-Dthreshold=20000 -Drootdir="/home/${DIR}" -Dbackend="esper" -e
	eval ${VOLTDB} &
	sleep 5
	ant run-w-vdb -Dthread=1 -Ddir="/home/${DIR}/git/esper-voter/" -Dfile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/0621/voltdbadhoc-ramp-1.txt" -Drate="1x${INPUT}" -Dcontestants=50 \
		-Dthreshold=20000 -Drootdir="/home/${DIR}" -Dbackend="voltdbadhoc" -e
	pkill -f 'java'
	eval ${VOLTDB} &
	sleep 5
	ant run-w-vdb -Dthread=1 -Ddir="/home/${DIR}/git/esper-voter/" -Dfile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/0621/voltdbsp-ramp-1.txt" -Drate="1x${INPUT}" -Dcontestants=50 \
		-Dthreshold=20000 -Drootdir="/home/${DIR}" -Dbackend="voltdb" -e
	pkill -f 'java'
	let INPUT+=200
done
