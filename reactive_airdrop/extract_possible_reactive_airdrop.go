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
// the program is about how to extract possible "reactive" data from the Ethereum raw data
// the program takes 4 parameters: the_begin_block_number, the_end_block_number, the output file to store the airdrop data related to standard "Transfer" event, the output file to store the airdrop data related to non-standard "Transfer" event

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
        //from address map, key is tx_hash, value is from address
        from_address_map := make(map[string]string)
        //to address map, key is tx_hash, value is to address
        to_address_map := make(map[string]string)
        //amount address map, key is tx_hash, value is amount
        amount_map := make(map[string]string)
        //data map, key is tx_hash, value is the length of the data
        data_map := make(map[string]int)
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
        
        //get the transactions
        bodykey := append(blockBodyPrefix, blockNumber...)
        bodykey = append(bodykey, blockHash...)

        bodydata, _ := db.Get(bodykey, nil)
        if len(bodydata) > 0 {
            body := new(types.Body)
            rlp.Decode(bytes.NewReader(bodydata), body)
            Txs := body.Transactions
            for _, tx := range Txs {
                var signer types.Signer
                if blocknum.Cmp(big.NewInt(1150000)) <= 0 {
                    signer = types.FrontierSigner{}
                } else if blocknum.Cmp(big.NewInt(2675000)) <= 0 {
                    signer = types.HomesteadSigner{}
                } else{
                    signer = types.NewEIP155Signer(big.NewInt(1))
                }
                msg, _ := tx.AsMessage(signer)
                tx_from := strings.ToLower(msg.From().String())
                tx_to := "0"
                if msg.To() == nil {
                    continue
                }else{
                    tx_to = strings.ToLower(msg.To().String())
                }
                value := new(big.Float).SetInt(msg.Value())
                dv_value := new(big.Float).Quo(value, decimals_float)
                float_value, _ := dv_value.Float64()
                if float_value == 0 {
                    continue
                }
                string_value := strconv.FormatFloat(float_value, 'f', -1, 64)
                tx_hash := strings.ToLower(tx.Hash().Hex())
                from_address_map[tx_hash] = tx_from
                to_address_map[tx_hash] = tx_to
                data_map[tx_hash] = len(tx.Data())
                amount_map[tx_hash] = string_value
            }
            blockReceiptKey := append(blockReceiptsPrefix, blockNumber...)
            blockReceiptKey = append(blockReceiptKey, blockHash...)
            blockReceiptData, _ :=db.Get(blockReceiptKey, nil)

            storageReceipts := []*types.ReceiptForStorage{}
            rlp.DecodeBytes(blockReceiptData, &storageReceipts)
            for _, receipt := range storageReceipts {
                Logs := (*types.Receipt)(receipt).Logs
                tx_hash := (*types.Receipt)(receipt).TxHash.Hex()
                //hash := strings.ToLower(common.HexToAddress(tx_hash.Hex()).String())
                if len(Logs) > 0 {
                    for _, Log := range Logs{
                        if len(Log.Topics) > 0 {
                            if Log.Topics[0].Hex() == logTransferSighash.Hex() {
                                ether_from, ok := from_address_map[tx_hash]
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
                                    if (ok) {
                                        ether_to, _ := to_address_map[tx_hash]
                                        ether_value, _ := amount_map[tx_hash]
                                        var transferEvent []string
                                        if data_map[tx_hash] == 0 {
                                            transferEvent = []string{ether_from, ether_to, ether_value, token_addr, from, to, amount.String(), strings.ToLower(tx_hash), blocktime.String(), blocknum.String()}
                                        } else {
                                            continue
                                        }
                                        
                                        writer1.Write(transferEvent)
                                    }
                                }
                            }
                        }
                    }
                    writer1.Flush()
                }
            }
        }
    }
}
