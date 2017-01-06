#!/bin/bash
cd /library/solr/data/blacklight/oai-solr-harvest
rm -f xml/*.xml

./oai.pl
./solr_deleteall.sh

echo '' > post.`date +"%Y%m%d"`.log

for x in `ls xml | grep xml`

do
	java -Durl=http://localhost:8983/solr/blacklight/update -jar post.jar  xml/$x &>> post.`date +"%Y%m%d"`.log 
echo "processing $x"
done
