# Explanation
This program is to extract the "Transfer" event data from the Ethereum raw data

# Usage
##Extract Token Transfer Event
Please put the related go files under your go workspace. Besides, you should also put the go-ethereum program under your go workspace, because our program use some libraries from go-ethereum

Besides, please change the ethereum chaindata path to your local chaindata path instead to open the leveldb
Then
```
go build extract_transfer_event.go
./extract_transfer_event from_block_number end_block_number output_file
```
from_block_number: the block number you would like to start to extract the data
end_block_number: the block number you would like to stop extracting the data
out_file: the possible reactive airdrop data that related to the standard "Transfer" event

The parameters in the output file:
token_addr: the token address
from: the token was transferred from this address
to: the token was transferred to this address
amount: the token amount
tx_hash: the transaction hash
blocktime: the block timestamp
blocknum: the block number

##Load the Events into Neo4j
The scheme of the Transfer Event in the neo4j is as followed:
(:NODE{address:$from})-[:TOKEN_TRANSFER{to:$to, value:toFloat($amount), time:toInt($blocktime)}]->(:NODE{address:$token_addr})

The variables have the same meaning as the above

First, we split the files into a list of small files for better processing. 
```
split -l length out_file transfer
python produce_file.py
```
replace the $length$, $out_file$, $transfer$ to your own local variable respectively. 
replace the neo4j user name and password in the produce_file.py correspondingly. 
