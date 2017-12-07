#!/bin/bash
#
# arg 1 -- k1
# arg 2 -- k2
#
if [ $# -ne 2 ]
then
  echo usage: firstsub lastsub
else
  let k1=$1
  let k2=$2
  let firstring=$1
  let lastring=$2
  while [ $((k1)) -le $((k2)) ]
  do
    echo whatever $((k1))
    echo file$((k1)).txt file$((k1))log.txt
#    python a_gettweets.py 1000 zzzzout.txt zzzzoutlog.txt
    python a_gettweets.py 1000 file$((k1)).txt logfile$((k1)).txt
    let k1=k1+1
    sleep 900
  done
fi
