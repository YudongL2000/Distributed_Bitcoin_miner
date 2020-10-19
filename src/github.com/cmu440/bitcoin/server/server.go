package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"encoding/json"
	"github.com/cmu440/lsp"
	"github.com/cmu440/bitcoin"
	"container/heap"
)

const maxUint = ^uint64(0)
const maxWorkload = 1
const reqIDWeight = 3

type pq []*job

type server struct {
	lspServer   lsp.Server
	droppedId   chan int
	clientReq   chan *clientRequest
	minerJoin   chan int
	minerRes    chan *newMinerResult
	miners      []*miner
	jobPQ       *pq
	reqList     []*clientRequest
	reqCount    int
	mainClose   chan bool
	readClose   chan bool
}

type miner struct {
	minerID    int
	avaliable  bool
	job        *job
}


type clientRequest struct {
	connID      int
	reqID       int
	data      string
	workingMiners []*miner
	pendingJobs   []*job
	lower     uint64
	upper     uint64
	result    *Message
	finished  bool
}

type job struct {
	reqID    int
	data     string
	lower 	 uint64
	upper  	 uint64
}

type newMinerResult struct{
	minerID   int
	hash      uint64
	nonce     uint64
}

func startServer(port int) (*server, error) {
	// TODO: implement this!
	params := lsp.NewParams()
    s, err := lsp.NewServer(port, params)
    if (err != nil){
		return nil, err
	}
	newServer := &server{
		lspServer:  s,
		droppedId:  make(chan int),
		clientReq:  make(chan *clientRequest),
		minerJoin:  make(chan int),
		minerRes:   make(chan *newMinerResult),
		miners:     nil,
		jobPQ:      nil,
		reqList:    nil,
		reqCount:   0,
		mainClose:  make(chan bool),
		readClose:  make(chan bool), 
    }
	return newServer, nil
}

var LOGF *log.Logger   



func (s *server) mainRoutine(){
	for {
		select{
		case <- s.mainClose:
			return 
		case req := <- s.clientReq:
			var splits uint64
			// append to client req list
			req.reqID = s.reqCount
			req.finished = false
			s.reqCount += 1
			s.reqList = append(s.reqList, req)			
			req.result = bitcoin.NewResult(maxUint, -1)
			// divide work and push to queue
			splits = (req.upper - req.lower - 1) / maxWorkload + 1
			var currLower uint64
			var currUpper uint64
			for j:= 0; j<splits; j++ {
				currLower = req.lower + j * maxWorkload
				if (j == splits-1) {
					currUpper = req.upper
				} else {
					currUpper = currLower + maxWorkload
				}
				newJob := &job{
					reqID:     req.reqID,
					data:      req.data,
					lower:     currLower,
					upper:     currUpper,
				}
				// add job to pendingJobs
				req.pendingJobs = append(req.pendingJobs, newJob)
				s.jobPQ = push(s.jobPQ, newJob)
			}
			s.assignJobs()

		case minerID := <-s.minerJoin:
			newMiner := &miner {
				minerID: minerID,
				available: true,
				job:       nil,
			}
			s.miners = append(s.miners, newMiner)
			//TODO: remove the first job from the priority queue and assign to the miner
			s.assignJobs()

		case minerRes := <- s.minerRes:
			// TODO: mark job finished, check if the client request is finished
			// TODO: assign new job to miner
			// Step1: check if miner still exists:
			for i, m := range s.miners {
				if m.minerID == minerRes.minerID {
					m.avaliable = true
					//Step2 : if exist, send result to clientRequest
					// for j, r := range s.reqList {
					// 	if r.reqID == m.job.reqID && !r.finished {
					// 		//Step3 : remove it from the working miners in the client request
					// 		if minerRes.hash < r.result.Hash {
					// 			r.result.Hash = minerRes.hash
					// 			r.result.Nonce = minerRes.nonce
					// 		}
					// 		for p, workingMiner := range r.workingMiners {
					// 			if workingMiner.minerID == m.minerID {
					// 				copy(r.workingMiners[p:], r.workingMiners[p+1:])
					// 				r.workingMiners = r.workingMiners[:len(r.workingMiners)-1]
					// 				break
					// 			}
					// 		}
					// 		//Step 4: check if the request is already accomplished
					// 		if (len(r.workingMiners)==0) && (len(r.pendingJobs)==0) {
					// 			connID := r.connID
					// 			payload, _ := json.Marshal(r.result)
					// 			s.lspServer.Write(connID, payload)
					// 			//Remove the request from list
					// 			// copy(s.reqList[j:], s.reqList[j+1:])
					// 			// s.reqList = s.reqList[:len(s.reqList)-1]
					// 			r.finished = true
					// 		}
					// 		break
					// 	}
					// }
					r := s.reqList[m.job.reqID]
					if !r.finished {
						//Step3 : remove it from the working miners in the client request
						if minerRes.hash < r.result.Hash {
							r.result.Hash = minerRes.hash
							r.result.Nonce = minerRes.nonce
						}
						for p, workingMiner := range r.workingMiners {
							if workingMiner.minerID == m.minerID {
								copy(r.workingMiners[p:], r.workingMiners[p+1:])
								r.workingMiners = r.workingMiners[:len(r.workingMiners)-1]
								break
							}
						}
						//Step 4: check if the request is already accomplished
						if (len(r.workingMiners)==0) && (len(r.pendingJobs)==0) {
							connID := r.connID
							payload, _ := json.Marshal(r.result)
							s.lspServer.Write(connID, payload)
							// mark request finished
							r.finished = true
						}
					}
					//Step5 : assign new jobs to the miner 
					s.assignJobs()
				}
			}
		case id := <- s.droppedId:
			// TODO: check the id against client and miner list / map
			// TODO: check if it is client or miner
			isMiner := false
			for i, m := range s.miners {
				if (m.minerID == id) {
					isMiner = true
					if !m.available {func priority()
						//Assigned with a job, push it back to PQ
						
						// TODO: add job to req pendingJobs
						// TODO: remove miner from req workingMiners
						for i, r := range s.reqList{
							if r.reqID == m.job.reqID && !r.finished {
								for j, workingMiner:= range r.workingMiners {
									if (workingMiner == m) {
										copy(r.workingMiners[j:], r.workingMiners[j+1:])
										r.workingMiners = r.workingMiners[:len(r.workingMiners)-1]
										break
									}
								}
								r.pendingJobs = append(r.pendingJobs, m.job)
								s.jobPQ = push(s.jobPQ, m.job)
								break
							}
						}
					}
					copy(s.miners[i:], s.miners[i+1:])
					s.miners = s.miners[:len(s.miners)-1]
					break
				}
			}
			// TODO: if it's a client ID, remove all requests associated with this client
			if !isMiner {
				for i, r := range s.reqList {
					if r.connID == id && !r.finished {
						for _, m := range r.workingMiners {
							m.available = true
						}
						s.assignJobs()
						// copy(s.reqList[i:], s.reqList[i+1:])
						// s.reqList = s.reqList[:len(s.reqList)-1]
						r.finished = true
						// TODO: verify "one connection per request"
						break
					}
				}
			}
		}
	}
}



func (s *server) assignJobs() {
	// find free miners
	for _, m := range s.miners {
		if m.available {
			// pop job from job list and assign to input miner
			var job *job
			job, s.jobPQ = pop(s.jobPQ)
			if m.jobPQ == nil {
				break
			}

			for _, r:= range s.reqList {
				if r.reqID != job.reqID || r.finished {
					continue
				}
				
				for idx,pendingJob := range r.pendingJobs {
					if pendingJob == job {
						// remove job from req pendingJobs
						copy(r.pendingJobs[idx:], r.pendingJobs[idx+1:])
						r.pendingJobs = r.pendingJobs[:len(r.pendingJobs)-1]
						// add miner to req workingMiners
						r.workingMiners = append(r.workingMiners, m)
						m.available = false
						m.job = job
						// write Request to miner
						msg := json.Marshal(NewRequest(job.data, job.lower, job.upper))
						s.lspServer.Write(m.minerID, msg)
						break
					}
				}
				break
			}
		}
	}
} 


func (s *server) readRoutine(){
	for {
		select {
		case <- s.readClose:
			return
		default:
			connID, bytes, err = s.lspServer.Read()
			if (err != nil) {
				// client / miner dropped
				droppedId <- connID
				continue
			}
			var msg bitcoin.Message
			infoErr := json.Unmarshal(bytes, &msg)
			if (infoErr == nil) {
				switch msg.Type {
				case bitcoin.Request:
					// new client request
					newClient := &clientRequest{
						connID:    connID,
						reqID:     -1,
						data:      msg.Data,
						lower:     msg.Lower,
						upper:     msg.Upper,
						minerIDs:  nil,
					}
					s.clientReq <- newClient

				case bitcoin.Join:
					// new miner
					s.minerJoin <- connID

				case bitcoin.Result:
					// result from miner
					res := &newMinerResult{
						minerID:  connID,
						hash:     msg.Hash,
						nonce:    msg.Nonce,
					}
					s.minerRes <- res
				}
			}
		}
	}
}



func main() {
	// You may need a logger for debug purpose
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)
	defer srv.lspServer.Close()

	go srv.readRoutine()
	srv.mainRoutine()
}

func score(j *job) {
	
	return 
}
// PQ functions
func push(queue *pq, j *job) {
	
	return append(queue, j)
}

func pop(queue *pq) (*job, *pq) {
	if len(queue) > 1 {
		return queue[0], queue[1:]
	} else if len(queue) == 1 {
		return queue[0], nil
	} else {
		return nil, nil
	}
}

func new() *pq {
	return nil
}
