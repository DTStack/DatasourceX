#!/usr/bin/env bash
echo 'source loader plugin building...'

mvn clean package -DskipTests -T 2 -U