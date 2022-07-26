package main

import (
	"fmt"
	"encoding/json"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
)

const maxUint = ^uint64(0)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	params := lsp.NewParams()
	client, err := lsp.NewClient(hostport, params)
	if err != nil {
		return nil, err
	}
	// send join request
	joinReq := bitcoin.NewJoin()
	payload, _ := json.Marshal(joinReq)
	client.Write(payload)
	return client, err
}

//calculates the actual result in the domain (lower, upper)
func calculationRoutine(req *bitcoin.Message, retChan chan *bitcoin.Message) {
	minHash := maxUint
	var minNonce uint64
	// calculate
	for nonce := req.Lower; nonce <= req.Upper; nonce++ {
		hash := bitcoin.Hash(req.Data, nonce)
		if hash <= minHash {
			minHash = hash
			minNonce = nonce
		}
	}
	// send result to main
	retChan <- bitcoin.NewResult(minHash, minNonce)
	return
}

func mainRoutine(m lsp.Client) {
	reqChan := make(chan *bitcoin.Message)
	retChan := make(chan *bitcoin.Message)
	closeChan := make(chan bool)
	jobCount := 0
	close := false

	go readRoutine(m, reqChan, closeChan)

	for {
		select {
		case req := <- reqChan:
			jobCount++
			// run calculations
			go calculationRoutine(req, retChan)

		case res := <- retChan:
			jobCount--
			if close && jobCount == 0 {
				return
			}
			payload, _ := json.Marshal(res)
			// write back to the server
			m.Write(payload)

		case <-closeChan:
			close = true
			if jobCount == 0 {
				return
			}
		}
	}
}

//read assigned jobs from the bitcoin server
func readRoutine(m lsp.Client, reqChan chan *bitcoin.Message, closeChan chan bool) {
	for {
		payload, err := m.Read()
		if err != nil {
			// should shut itself down in this case
			closeChan <- true
			return
		}

		var req bitcoin.Message
		infoErr := json.Unmarshal(payload, &req)
		if infoErr == nil {
			reqChan <- &req
		}
	}
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}
	fmt.Println("Joined with server at port", hostport)
	defer miner.Close()

	mainRoutine(miner)
}
