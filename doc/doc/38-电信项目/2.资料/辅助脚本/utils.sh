#!/bin/bash
for i in admin@linux01 admin@linux02 admin@linux03
do
	echo "================           $i             ================"
	ssh $i 'jps'
done
