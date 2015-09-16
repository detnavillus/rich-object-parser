#set -vx
export cp class solrHost solrPort collection query fields format handler rows nLines file
class=com.lucidworks.solr.client.StreamingRequestClient
solrHost=localhost
solrPort=8983
collection=collection1
fields=id,text
query=*:*
handler=exportTV
rows=1000
nLines=10000
file=/Users/tedsullivan/Desktop/LucidWorks/ResearchProjects/StreamingRequestHandler/testData/testOut

cp=.
cp=$cp:jars/streaming-req-handler-1.0-SNAPSHOT.jar
cp=$cp:jars/httpcore-4.3.jar
cp=$cp:jars/httpclient-4.3.1.jar
cp=$cp:jars/commons-logging-1.1.3.jar
cp=$cp:jars/commons-io-1.3.2.jar

java -cp $cp $class -solrHost $solrHost -solrPort $solrPort -collection $collection -query $query -handler $handler -fields $fields -rows $rows -nLines $nLines -file $file
