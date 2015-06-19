#!/bin/bash

trap "exit" INT
INPUT=200
FINAL=3000
DIR="jlmeehan"
#rm "/home/${DIR}/git/esper-voter/data/out/out.txt"

while [ "${INPUT}" -lt "${FINAL}" ];
do
	ant run -Dthread=1 -Ddir="/home/${DIR}/git/esper-voter/" -Dfile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/0619/esper-ramp-1.txt" -Drate="1x${INPUT}" -Dcontestants=50 \
		-Dthreshold=20000 -Drootdir="/home/${DIR}" -Dbackend="esper" -e
	ant run -Dthread=1 -Ddir="/home/${DIR}/git/esper-voter/" -Dfile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/0619/voltdbadhoc-ramp-1.txt" -Drate="1x${INPUT}" -Dcontestants=50 \
		-Dthreshold=20000 -Drootdir="/home/${DIR}" -Dbackend="voltdbadhoc" -e
	ant run -Dthread=1 -Ddir="/home/${DIR}/git/esper-voter/" -Dfile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/0619/voltdbsp-ramp-1.txt" -Drate="1x${INPUT}" -Dcontestants=50 \
		-Dthreshold=20000 -Drootdir="/home/${DIR}" -Dbackend="voltdb" -e
	let INPUT+=100
done
