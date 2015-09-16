#set -vx
export cp class solrHost solrPort collection query fields handler rows nLines file writeSchema
class=com.lucidworks.solr.client.StreamingExportClient
solrHost=localhost
solrPort=8983
collection=collection1
fields=id,address_s,compName_s
query=*:*
handler=exportCSV
writeSchema=true
nLines=4
file=/Users/tedsullivan/Desktop/LucidWorks/ResearchProjects/StreamingRequestHandler/testData/testExport

cp=.
cp=$cp:jars/streaming-req-handler-1.0-SNAPSHOT.jar
cp=$cp:jars/httpcore-4.3.jar
cp=$cp:jars/httpclient-4.3.1.jar
cp=$cp:jars/commons-logging-1.1.3.jar
cp=$cp:jars/commons-io-1.3.2.jar

java -cp $cp $class -solrHost $solrHost -solrPort $solrPort -collection $collection -query $query -handler $handler -fields $fields -nLines $nLines -file $file -writeSchema $writeSchema
