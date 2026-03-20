#!/bin/bash
# Use: scp ./vt_wrapper.sh host:/mnt/local/shared/vt_wrapper.sh/vt_wrapper.sh
"$@" &
pid=$!
wait $pid
status=$?
echo "[$(date)] PID $pid exited with status $status" >> /var/log/monitor.log
if [ $status -gt 128 ]; then
  echo "Signal: $((status - 128))" >> /var/log/monitor.log
fi

