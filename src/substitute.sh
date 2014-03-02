# Copy file from source to target while substituting certain values

SOURCE=$1
TARGET=$2
RULE=$3

cat $SOURCE | sed -e $RULE > $TARGET
