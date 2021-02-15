#!/bin/bash


rm mr-*
rm temp-*

cd ../mrapps && go build $RACE -buildmode=plugin wc.go || exit 1

cd ../main && go build mrcoordinator.go && go build mrworker.go

./mrcoordinator pg*txt &
./mrworker ../mrapps/wc.so &
./mrworker ../mrapps/wc.so &
./mrworker ../mrapps/wc.so

