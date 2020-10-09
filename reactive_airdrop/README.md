# Explanation
extract possible reactive airdrop data from Ethereum raw data

# Usage
Please put the related go files under your go workspace. Besides, you should also put the go-ethereum program under your go workspace, because our program use some libraries from go-ethereum

Besides, please change the ethereum chaindata path to your local chaindata path instead to open the leveldb
Then
```
go build extract_possible_reactive_airdrop.go
./extract_possible_reactive_airdrop from_block_number end_block_number output_file
```
from_block_number: the block number you would like to start to extract the data
end_block_number: the block number you would like to stop extracting the data
out_file: the possible reactive airdrop data that related to the standard "Transfer" event

The parameters in the output file:
ether_from: the ether was transferred from this address
ether_to: the ether was transferred to this address
ether_value: the amount of ether
token_addr: the token address
from: the token was transferred from this address
to: the token was transferred to this address
amount: the token amount
tx_hash: the transaction hash
blocktime: the block timestamp
blocknum: the block number
