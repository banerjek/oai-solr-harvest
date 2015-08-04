#!/bin/bash
rm xml/*.xml

./oai.pl

echo '' > post.`date +"%Y%m%d"`.log

for x in `ls xml | grep xml`

do
	java -Durl=http://localhost:8080/solr/blacklight/update -jar post.jar  xml/$x &>> post.`date +"%Y%m%d"`.log 
echo "processing $x"
done
