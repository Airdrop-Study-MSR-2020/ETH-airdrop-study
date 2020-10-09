package main

import (
	"strings"
	"encoding/binary"
	"encoding/csv"
    "strconv"
    "os"
    "bytes"

    "math/big"
	"github.com/ethereum/go-ethereum/core/types"
    "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/syndtr/goleveldb/leveldb"
    "github.com/ethereum/go-ethereum/crypto"
)

var (
	headerPrefix = []byte("h") // headerPrefix + num (uint64 big endian) + hash -> header
	numSuffix    = []byte("n") // headerPrefix + num (uint64 big endian) + numSuffix -> hash
    blockBodyPrefix = []byte("b")
    blockReceiptsPrefix = []byte("r")
    logTransferSig = []byte("Transfer(address,address,uint256)")
    logTransferSighash = crypto.Keccak256Hash(logTransferSig)
    decimals, _ = new(big.Int).SetString("1000000000000000000", 10)
    decimals_float = new(big.Float).SetInt(decimals)
)
// the program is about how to extract token "Transfer" events from the Ethereum raw data
// the program takes 3 parameters: the_begin_block_number, the_end_block_number, the output file to store the airdrop data related to standard "Transfer" event

func main() {

	// Connection to leveldb
	  db, _ := leveldb.OpenFile("/home/name/.ethereum/geth/chaindata", nil) // change the path to your local chaindata path
    file1, _ := os.OpenFile(os.Args[3], os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
    
    defer file1.Close()
    
    writer1 := csv.NewWriter(file1)
    
    defer writer1.Flush()
    
    from_block_num, _ := strconv.Atoi(os.Args[1])
    end_block_num, _ := strconv.Atoi(os.Args[2])
    
    for i:= from_block_num; i< end_block_num; i++ {
        //block number
        blockNumber := make([]byte, 8)
        binary.BigEndian.PutUint64(blockNumber, uint64(i))
        //block hash
        hashKey := append(headerPrefix, blockNumber...)
        hashKey = append(hashKey, numSuffix...)

        blockHash, _ := db.Get(hashKey, nil)

        //headerkey
        headerkey := append(headerPrefix, blockNumber...)
        headerkey = append(headerkey, blockHash...)
        //get block header, then I could get the block time and blocknum
        blockHeaderData, _ := db.Get(headerkey, nil)
        header := new(types.Header)
        rlp.Decode(bytes.NewReader(blockHeaderData), header)
        blocktime := header.Time
        blocknum := header.Number
        
        blockReceiptKey := append(blockReceiptsPrefix, blockNumber...)
        blockReceiptKey = append(blockReceiptKey, blockHash...)
        blockReceiptData, _ :=db.Get(blockReceiptKey, nil)

        storageReceipts := []*types.ReceiptForStorage{}
        rlp.DecodeBytes(blockReceiptData, &storageReceipts)
        for _, receipt := range storageReceipts {
            Logs := (*types.Receipt)(receipt).Logs
            tx_hash := (*types.Receipt)(receipt).TxHash.Hex()
            if len(Logs) > 0 {
                for _, Log := range Logs{
                    if len(Log.Topics) > 0 {
                        if Log.Topics[0].Hex() == logTransferSighash.Hex() {
                            from := ""
                            to := ""
                            amount := new(big.Int)
                            if len(Log.Topics) > 2 {
                                from = strings.ToLower(common.HexToAddress(Log.Topics[1].Hex()).String())
                                to = strings.ToLower(common.HexToAddress(Log.Topics[2].Hex()).String())
                                amount.SetBytes(Log.Data)
                            } else{
                                length := len(Log.Data)/3
                                from = strings.ToLower(common.BytesToHash(Log.Data[0:length]).Hex())
                                if len(from) < 40 {
                                    continue
                                }
                                from = "0x"+from[len(from)-40:]
                                to = strings.ToLower(common.BytesToHash(Log.Data[length:2*length]).Hex())
                                to = "0x"+to[len(to)-40:]
                                amount.SetBytes(Log.Data[2*length:])
                            }
                            if from != ""  && to != "" {
                                token_addr := strings.ToLower(Log.Address.String())
                                var transferEvent []string
                                transferEvent = []string{token_addr, from, to, amount.String(), strings.ToLower(tx_hash), blocktime.String(), blocknum.String()}
                                        
                                writer1.Write(transferEvent)
                            }
                        }
                    }
                    writer1.Flush()
                }
            }
        }
    }
}
