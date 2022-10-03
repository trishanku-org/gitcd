#!/bin/bash

LABEL="trishanku=the-hard-way"

CONTAINERS=$(docker ps -a --filter "label=$LABEL" --format "{{.Names}}")

echo $CONTAINERS | sed 's/ /\n/g' | grep -v gitcd | grep -v kube-apiserver | xargs docker stop
echo $CONTAINERS | sed 's/ /\n/g' | grep -e kube-apiserver | xargs docker stop
echo $CONTAINERS | sed 's/ /\n/g' | grep -e gitcd | xargs docker stop

echo $CONTAINERS | sed 's/ /\n/g' | xargs docker rm $CONTAINERS

docker volume list --filter "label=$LABEL" --format "{{.Name}}" | xargs docker volume rm
