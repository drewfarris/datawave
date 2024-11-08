#!/bin/bash

# maven must be configured to use a token for the github-datawave package repository with write:packages permissions

curl --output splade-pp-ed-vocab.txt https://rgw.cs.uwaterloo.ca/pyserini/data/wordpiece-vocab.txt && \
mvn deploy:deploy-file \
  -DrepositoryId=github-datawave \
  -Durl=https://maven.pkg.github.com/NationalSecurityAgency/datawave \
  -Dfile=splade-pp-ed-vocab.txt \
  -DartifactId=splade-pp-ed-vocab \
  -DgroupId=gov.nsa.datawave.sparse \
  -Dversion=1
 
