#!/bin/bash

trap "exit" INT
INPUT=250
FINAL=5000
DIR="john"
rm "/home/${DIR}/git/esper-voter/data/out/out.txt"

while [ "${INPUT}" -lt "${FINAL}" ];
do
	ant run -Dthreads=1 -Dvotedir="/home/${DIR}/git/esper-voter/data/" -Dvotefile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/1-thread-serial.txt" -Dinputrate="${INPUT}" -Dcontestants=50 -Dthreshold=20000 -e
	let INPUT+=250
done

let INPUT=250

while [ "${INPUT}" -lt "${FINAL}" ];
do
	ant run -Dthreads=1 -Dvotedir="/home/${DIR}/git/esper-voter/data/" -Dvotefile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/1-thread.txt" -Dinputrate="${INPUT}" -Dcontestants=50 -Dthreshold=20000 -Dnoorder="true" -e
	let INPUT+=250
done

let INPUT=250

while [ "${INPUT}" -lt "${FINAL}" ];
do
	ant run -Dthreads=5 -Dvotedir="/home/${DIR}/git/esper-voter/data/" -Dvotefile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/5-thread-serial.txt" -Dinputrate="${INPUT}" -Dcontestants=50 -Dthreshold=20000 -e
	let INPUT+=250
done

let INPUT=250

while [ "${INPUT}" -lt "${FINAL}" ];
do
	ant run -Dthreads=5 -Dvotedir="/home/${DIR}/git/esper-voter/data/" -Dvotefile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/5-thread.txt" -Dinputrate="${INPUT}" -Dcontestants=50 -Dthreshold=20000 -Dnoorder="true" -e
	let INPUT+=250
done

let INPUT=250

while [ "${INPUT}" -lt "${FINAL}" ];
do
	ant run -Dthreads=10 -Dvotedir="/home/${DIR}/git/esper-voter/data/" -Dvotefile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/10-thread-serial.txt" -Dinputrate="${INPUT}" -Dcontestants=50 -Dthreshold=20000 -e
	let INPUT+=250
done

let INPUT=250

while [ "${INPUT}" -lt "${FINAL}" ];
do
	ant run -Dthreads=10 -Dvotedir="/home/${DIR}/git/esper-voter/data/" -Dvotefile="votes-50-20000_1.txt" \
		-Doutfile="/home/${DIR}/git/esper-voter/data/out/10-thread.txt" -Dinputrate="${INPUT}" -Dcontestants=50 -Dthreshold=20000 -Dnoorder="true" -e
	let INPUT+=250
done
