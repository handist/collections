#!/usr/bin/gnuplot

# Parameters
# 1. Input file
# 2. Output file
# 3. Number of hosts in the input file
# 4. Nb of Columns in multiplot

INPUT = ARG1
OUTPUT = ARG2
NB_HOST = ARG3
COLS = ARG4

reset
set terminal pngcairo size 1000,700 enh font ",18"

set output OUTPUT

set multiplot layout COLS,COLS

set xrange [0:*]
set yrange [0:*]

set style line 1 lc rgb 'black'
set style line 2 lc rgb 'black'
set style line 3 lc rgb 'black'
set style fill solid 

do for [h=0:NB_HOST-1] {
plot INPUT index h using 1:(sum [col=2:4] column(col)) notitle with fillsteps lc rgb 'red', \
     '' index h using 1:(sum [col=3:4] column(col)) notitle with fillsteps lc rgb 'grey', \
     '' index h using 1:(sum [col=4:4] column(col)) notitle with fillsteps lc rgb 'green'
}


