#!/bin/bash

BASEDIR=$(dirname "$0")

echo "{
  \"lastUpdated\" : `date +%s0000`,
  \"parcels\" : ["

FIRST=1

for file in build/*.parcel; do
  if [[ $FIRST -eq 1 ]]; then
    FIRST=0
  else
    echo "    ,"
  fi

  echo "    {
        \"parcelName\" : \"`basename $file`\",
        \"components\" : [],
        \"depends\": \"CDH (>= 6.0), CDH (<< 7.0)\",
        \"hash\" : \"`$BASEDIR/hashFile.sh $file`\"
      }"
done

echo "  ]
}"
