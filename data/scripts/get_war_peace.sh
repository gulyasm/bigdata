#!/bin/bash

get http://www.textfiles.com/etext/FICTION/warpeace.txt
hdfs dfs -mkdir /data
hdfs dfs -put -f warpeace.txt /data/warpeace.txt
