#!/bin/bash

helm package .
helm push migrations-operator-0.0.1.tgz oci://471112736248.dkr.ecr.us-east-2.amazonaws.com

exit 0;
