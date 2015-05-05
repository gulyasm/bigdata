# Hadoop Streaming Python example

## Usage
To run it on a hadoop cluster:
`hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files mapper.py,reducer.py -input /data/warpeace.txt -output /output -mapper mapper.py -reducer reducer.py`

To test it in shell:
`cat warpeace.txt | ./mapper.py | sort -k1,1 | ./reducer.py`
