#!/bin/bash

trap "exit" INT
INPUT=200
FINAL=3100
DIR="jlmeehan"
VOLTDB="/home/${DIR}/git/voltdb/bin/voltdb create --deployment=/home/${DIR}/git/esper-voter/voter-benchmark/voltdbconfig.xml --host=localhost --http=8083"
#rm "/home/${DIR}/git/esper-voter/data/out/out.txt"

while [ "${INPUT}" -lt "${FINAL}" ];
do
	ant run -Dthread=1 -Ddir="/home/${DIR}/git/esper-voter/" -Dfile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/0622/esper-ramp-log-1.txt" -Drate="1x${INPUT}" -Dcontestants=50 \
		-Dthreshold=20000 -Drootdir="/home/${DIR}" -Dbackend="esper" -e -Dlog="true"
	eval ${VOLTDB} &
	sleep 5
	ant run-w-vdb -Dthread=1 -Ddir="/home/${DIR}/git/esper-voter/" -Dfile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/0622/voltdbadhoc-ramp-log-1.txt" -Drate="1x${INPUT}" -Dcontestants=50 \
		-Dthreshold=20000 -Drootdir="/home/${DIR}" -Dbackend="voltdbadhoc" -e  -Dlog="true"
	pkill -f 'java'
	eval ${VOLTDB} &
	sleep 5
	ant run-w-vdb -Dthread=1 -Ddir="/home/${DIR}/git/esper-voter/" -Dfile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/0622/voltdbsp-ramp-log-1.txt" -Drate="1x${INPUT}" -Dcontestants=50 \
		-Dthreshold=20000 -Drootdir="/home/${DIR}" -Dbackend="voltdb" -e  -Dlog="true"
	pkill -f 'java'
	let INPUT+=200
done

cd "/home/${DIR}/git/s-store"

python ./tools/autorunexp-3.py -p "voterdemosstorefile" -o "experiments/0622/voterdemosstore-1c-95-0622-site3.txt" \
--txnthreshold 0.95 -e "experiments/0622/site3-voterdemosstore-nolog-1.txt" --winconfig "(site3) s-store regular" \
--threads 1 --rmin 200 --rmax 3100 --rstep 200 --finalrstep 200  --warmup 10000 --numruns 1 --recordramp