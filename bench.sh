#!/usr/bin/env bash

set -e

logsfile=logs-queries.jsonl
duration=600s

while getopts ":ld:" option; do
	case $option in
		l)
			logsfile=$OPTARG;;
		d)
			duration=$OPTARG;;
		\?)
			echo "Error: Invalid option"
			exit;;
	esac
done

run() {
	echo "--------------------------------------"
	echo "-> $3"
	echo ''

	filter="$2 | {method:\"POST\",url:\"http://$1\",header:{\"Content-Type\":[\"application/json\"]},body:.|@base64}"
	jq -c "$filter" "$logsfile" | vegeta attack -name="$3" -lazy -format=json -duration="$duration" | vegeta report --type=text

	echo ''
	echo ''
}

raw_request='.line | fromjson | .request.params[0]'
rpc_request="$raw_request | {jsonrpc:\"2.0\",method:\"eth_getLogs\",id:42,params:[.]}"

run '127.0.0.1:8002/rpc' "$raw_request" Oracle
run '10.0.0.1:8545'      "$rpc_request" Erigon
