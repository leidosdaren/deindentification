#!/bin/bash
set -eo pipefail
gradle -q packageLibs
mv build/distributions/deid-java.zip build/deid-java-lib.zip