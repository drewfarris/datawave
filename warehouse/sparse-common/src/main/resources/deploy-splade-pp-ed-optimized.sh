#!/bin/bash

# maven must be configured to use a token for the github-datawave package repository with write:packages permissions

curl --output splade-pp-ed-optimized.onnx https://rgw.cs.uwaterloo.ca/pyserini/data/splade-pp-ed-optimized.onnx && \
mvn deploy:deploy-file \
  -DrepositoryId=github-datawave \
  -Durl=https://maven.pkg.github.com/NationalSecurityAgency/datawave \
  -Dfile=splade-pp-ed-optimized.onnx \
  -DartifactId=splade-pp-ed-optimized \
  -DgroupId=gov.nsa.datawave.sparse \
  -Dversion=1
 
