#!/bin/bash

DATE=$1
GBN=$2
MAJOR_VERSION=$3
VERSION=$4
REF=$5

FIRST=1
OS="unknown"

getOs() {
    filename=$1
    OS=`echo $filename | cut -d'-' -f 4 | cut -d'.' -f 1`;
}


echo "{
  \"build_time\": \"$DATE\",
  \"gbn\": $GBN,
  \"product\": \"sqoop-netezza-connector\",
  \"schema_version\": 2,
  \"version\": \"$VERSION\",
  \"sources\": [
    {
      \"ref\": \"$REF\",
      \"repo\": \"http://github.mtv.cloudera.com/CDH/sqoop-connectors.git\"
    }
  ],
  \"output\": ["
    for file in build/*.parcel; do
      if [[ $FIRST -eq 1 ]]; then
        FIRST=0
      else
        echo "    ,"
      fi
      
      getOs $file
      echo "    {
            \"base\" : \"sqoop-netezza-connector$MAJOR_VERSION/$VERSION/parcels\",
            \"component\" :  \"sqoop-connectors\",
            \"files\": [
                        \"`basename $file`\",
                        \"`basename $file`.sha1\"
            ],
            \"os\": \"$OS\",
            \"type\": \"parcels\"
      }"
      
    done
  
echo "  ]
}"
