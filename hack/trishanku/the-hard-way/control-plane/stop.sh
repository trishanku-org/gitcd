#!/bin/bash

LABEL="trishanku=the-hard-way"

CONTAINERS=$(docker ps -a --filter "label=$LABEL" --format "{{.Names}}")

echo $CONTAINERS | sed 's/ /\n/g' | grep -v gitcd | grep -v kube-apiserver | xargs docker stop
echo $CONTAINERS | sed 's/ /\n/g' | grep kube-apiserver | xargs docker stop
echo $CONTAINERS | sed 's/ /\n/g' | grep gitcd | xargs docker stop
