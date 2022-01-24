#!/bin/bash

set -x

docker run -d --name registry -p 6000:5000 registry:2 && \
    docker tag trishanku/gitcd:latest localhost:6000/trishanku/gitcd:latest && \
    docker push localhost:6000/trishanku/gitcd:latest