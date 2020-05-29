# musicstreamingservice
Definition of trending: It may be defined loosely as those songs that have gathered relatively high number of streams within small time window (e.g. the last four hours) and have also shown positive increases in their stream growth rates.<br/><br/>

The dataset is hosted on Amazon S3 and is 44GB in size. Each stream is represented as a tuple with the following attributes:<br/>
(song ID, user ID, timestamp, hour, date)<br/>
Each tuple consists of the song ID of the streamed song, the user ID of the user who streamed the song, the timestamp (Unix) of the stream, the hour of streaming, and the date of streaming.<br/><br/>

The algorithm is designed keeping in mind the following points:<br/>
1. It should take into account the recent history.<br/>
2. Songs with random streaming patterns with sudden spikes are not labelled as trending, as these might be anomalies in the data.<br/><br/>

The algorithm implemented here makes use of decaying window algorithm with a window size of a day. The final output is a list of top 100 trending songs for 25th-31st Dec.


