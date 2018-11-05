#!/bin/bash
while read line
do
	./launch.py $line 2>/dev/null
done < "${1:-/dev/stdin}"
