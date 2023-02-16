# Quick & Dirty clickhouse TCP proxy
This project is a query & dirty TCP proxy for clickhouse in order to chose the best approach for the TCP feature in the project chproxy

#How To use
first run the proxy (it assumes there is a local clickhouse server running on port 9000):
go build; ./proxy
then run your clickhouse-client on port 9001
clickhouse-client --port 9001

#outcomes
##speed
using the decoding logic of ch-go reduces a LOT the processing speed (for a query returning 25 columns): 
* 47.5 sec using the decoding logic of ch-go (sniffAndCopyStreamV2)
* 3.7 -10 sec (the first run takes 3.7 and the next ones 5-10 sec, likely a gargabe collector issue) using a dummy logic that doesn't decode payloads (sniffAndCopyStreamV1)
* 3.5 sec without using a proxy
=> we can rely 100% on the code of ch-go


processing speed for the given query (only one run)
`select number, number*number, toString(sipHash128(number)),sipHash64(number) from system.numbers limit 200000000`
* 34 sec
* 19 sec
* 18 sec