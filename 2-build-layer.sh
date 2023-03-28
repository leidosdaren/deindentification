#!/bin/bash
set -eo pipefail
gradle -q packageLibs
mv build/distributions/deidentification.zip build/deid-java-lib.zip