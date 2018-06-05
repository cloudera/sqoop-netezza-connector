#!/bin/bash

set -ex

RELEASEDIR=$1
GBN=$2
TAGS="release official"

VE="$RELEASEDIR/../.ve"
virtualenv "$VE"
source "$VE/bin/activate"

sudo pip install --upgrade pip
sudo pip install --index-url=https://pypi.infra.cloudera.com/api/pypi/cloudera/simple buildinfo
sudo pip install -e 'git://github.mtv.cloudera.com/CDH/cdh.git@cdh6.x#egg=cdh-cauldron&subdirectory=./lib/python/cauldron'

cd $RELEASEDIR

upload s3 . --base "build/$GBN"

for tag in $TAGS; do
    buildinfo addtag "$GBN" "$tag"
done

buildinfo dumpjson "$GBN"

echo "Proxy URL: http://cloudera-build-us-west-1.vpc.cloudera.com/s3/build/$GBN/"
