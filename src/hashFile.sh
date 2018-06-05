#!/bin/bash

sha1sum $1 | cut -f1 -d' '
