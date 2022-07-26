/*
 * Scheduler documentation
 *
 * We divide every client request into smaller jobs with length no more than 10000 to achieve
 * equal load for all miners. These jobs are then kept within a priority queue (implemented by
 * slice); the priority of a job is calculated based on the number of pending jobs of the
 * request that this job belongs to, and how early this request arrived, which can be determined
 * by a unique request ID we keep for each request that comes in. When we "push" a job onto the
 * queue, we are really inserting it into the right location based on the score (priority) we
 * calculate (See func score and func pushJob).
 * Whenever a miner becomes available, or a new job is pushed onto the queue, we pop the first
 * job off and assign it to free miners so that we can either keep the queue empty or keep all
 * miners busy.
 */

package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"os"
	"strconv"
)

const maxUint = ^uint64(0) // int max
const maxWorkload = 10000  // the maximum workload each job can take
const reqIDWeight = 3      // the weight of connection id when computing priority
const workloadWeight = 10  // the weight of workload when computing priority

type server struct {
	lspServer     lsp.Server           // the lsp server
	droppedIdChan chan int             // channel for passing the dropped client / miner ConnID
	clientReqChan chan *clientRequest  // the channel for passing client requests to mainRoutine
	minerJoinChan chan int             // the channel for passing connection ID of the new joined miner
	minerResChan  chan *newMinerResult // the channel for passing the new computation results sent out by a miner
	miners        []*miner             // the list of connected miners
	jobPQ         []*job               // the priority queue for pending jobs obtained from client requests
	reqList       []*clientRequest     // list of client request
	reqCount      int                  // the client request ID counter (used for assigning new request ID)
}

type miner struct {
	minerID   int
	available bool
	job       *job
}

type clientRequest struct {
	connID        int              // connection ID of the client that sent the request
	reqID         int              // assigned request ID to the request according to the order they're received
	data          string           // the date payload string for hash computation
	workingMiners []*miner         // the number of miners working for this request
	pendingJobs   []*job           // the split jobs of this request that hasn't been taken by miners
	lower         uint64           // the lower of client request
	upper         uint64           // the upper of the client request
	result        *bitcoin.Message // the result message to send back to client
	finished      bool             // whether the request is finished
}

type job struct {
	reqID int    // request ID of the request this job come from
	data  string // the payload data for computation
	lower uint64 // the lower of the input
	upper uint64 // the upper of the input
}

type newMinerResult struct {
	minerID int    // the conn ID of the miner
	hash    uint64 // the min hashed value
	nonce   uint64 // the nonce of the result
}

func startServer(port int) (*server, error) {
	params := lsp.NewParams()
	s, err := lsp.NewServer(port, params)
	if err != nil {
		return nil, err
	}
	newServer := &server{
		lspServer:     s,
		droppedIdChan: make(chan int),
		clientReqChan: make(chan *clientRequest),
		minerJoinChan: make(chan int),
		minerResChan:  make(chan *newMinerResult),
		miners:        nil,
		jobPQ:         nil,
		reqList:       nil,
		reqCount:      0,
	}
	return newServer, nil
}

var LOGF *log.Logger

func (s *server) mainRoutine() {
	for {
		select {
		case req := <-s.clientReqChan:
			var splits uint64
			// append to client req list
			req.reqID = s.reqCount
			req.finished = false
			s.reqCount += 1
			s.reqList = append(s.reqList, req)
			// divide work and push to queue
			splits = (req.upper-req.lower)/maxWorkload + 1
			var currLower, currUpper uint64
			for j := uint64(0); j < splits; j++ {
				currLower = req.lower + (j * maxWorkload)
				if j == splits-1 {
					currUpper = req.upper
				} else {
					currUpper = currLower + maxWorkload - 1
				}
				newJob := &job{
					reqID: req.reqID,
					data:  req.data,
					lower: currLower,
					upper: currUpper,
				}
				// add job to pendingJobs
				req.pendingJobs = append(req.pendingJobs, newJob)
				s.pushJob(newJob)
			}
			s.assignJobs()

		case minerID := <-s.minerJoinChan:
			newMiner := &miner{
				minerID:   minerID,
				available: true,
				job:       nil,
			}
			s.miners = append(s.miners, newMiner)
			// remove the first job from the priority queue and assign to the miner
			s.assignJobs()

		case minerRes := <-s.minerResChan:
			// mark job finished, check if the client request is finished
			// assign new job to miner
			// Step 1: check if miner still exists:
			for _, m := range s.miners {
				if m.minerID == minerRes.minerID {
					m.available = true
					r := s.reqList[m.job.reqID]
					if !r.finished {
						// Step 2: remove it from the working miners in the client request
						if r.result == nil || minerRes.hash < r.result.Hash {
							r.result = bitcoin.NewResult(minerRes.hash, minerRes.nonce)
						}
						for p, workingMiner := range r.workingMiners {
							if workingMiner.minerID == m.minerID {
								copy(r.workingMiners[p:], r.workingMiners[p+1:])
								r.workingMiners = r.workingMiners[:len(r.workingMiners)-1]
								break
							}
						}
						//Step 3: check if the request is already accomplished
						if (len(r.workingMiners) == 0) && (len(r.pendingJobs) == 0) {
							connID := r.connID
							payload, _ := json.Marshal(r.result)
							s.lspServer.Write(connID, payload)
							// mark request finished
							r.finished = true
						}
					}
					//Step 4: assign new jobs to the miner
					s.assignJobs()
				}
			}

		case id := <-s.droppedIdChan:
			// check the id against client and miner list
			// check if it is client or miner
			isMiner := false
			for i, m := range s.miners {
				if m.minerID == id {
					isMiner = true
					if !m.available { // assigned with a job, push it back to PQ
						// add job to req pendingJobs
						// remove miner from req workingMiners
						r := s.reqList[m.job.reqID]
						if !r.finished {
							for j, workingMiner := range r.workingMiners {
								if workingMiner == m {
									copy(r.workingMiners[j:], r.workingMiners[j+1:])
									r.workingMiners = r.workingMiners[:len(r.workingMiners)-1]
									break
								}
							}
							r.pendingJobs = append(r.pendingJobs, m.job)
							s.pushJob(m.job)
							s.assignJobs()
						}
					}
					copy(s.miners[i:], s.miners[i+1:])
					s.miners = s.miners[:len(s.miners)-1]
					break
				}
			}
			// if it's a client ID, remove all requests associated with this client
			if !isMiner {
				for _, r := range s.reqList {
					if r.connID == id && !r.finished {
						for _, m := range r.workingMiners {
							m.available = true
						}
						s.assignJobs()
						r.finished = true
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
		for m.available && len(s.jobPQ) > 0 {
			// pop job from job list and assign to input miner
			job := s.popJob()

			r := s.reqList[job.reqID]
			if r.finished {
				continue
			}
			for idx, pendingJob := range r.pendingJobs {
				if pendingJob == job {
					// remove job from req pendingJobs
					copy(r.pendingJobs[idx:], r.pendingJobs[idx+1:])
					r.pendingJobs = r.pendingJobs[:len(r.pendingJobs)-1]
					// add miner to req workingMiners
					r.workingMiners = append(r.workingMiners, m)
					m.available = false
					m.job = job
					// write Request to miner
					msg, _ := json.Marshal(bitcoin.NewRequest(job.data, job.lower, job.upper))
					s.lspServer.Write(m.minerID, msg)
					break
				}
			}
		}
	}
}

func (s *server) readRoutine() {
	for {
		connID, bytes, err := s.lspServer.Read()
		if err != nil {
			// client / miner dropped
			s.droppedIdChan <- connID
			continue
		}
		var msg bitcoin.Message
		infoErr := json.Unmarshal(bytes, &msg)
		if infoErr == nil {
			switch msg.Type {
			case bitcoin.Request:
				// new client request
				newClient := &clientRequest{
					connID:        connID,
					reqID:         -1,
					data:          msg.Data,
					lower:         msg.Lower,
					upper:         msg.Upper,
					workingMiners: nil,
					pendingJobs:   nil,
					finished:      false,
					result:        nil,
				}
				s.clientReqChan <- newClient
			case bitcoin.Join:
				// new miner
				s.minerJoinChan <- connID
			case bitcoin.Result:
				// result from miner
				res := &newMinerResult{
					minerID: connID,
					hash:    msg.Hash,
					nonce:   msg.Nonce,
				}
				s.minerResChan <- res
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
		return
	}
	fmt.Println("Server listening on port", port)
	defer srv.lspServer.Close()

	go srv.readRoutine()
	srv.mainRoutine()
}

// score in priority queue for insertion (smaller score corresponds to higher priority)
func (s *server) score(j *job) int {
	req := s.reqList[j.reqID]
	score := (reqIDWeight * j.reqID) + (workloadWeight * len(req.pendingJobs))
	return score
}

// insert job into the priority queue
func (s *server) pushJob(j *job) {
	newQ := make([]*job, len(s.jobPQ)+1)
	inserted := false
	for idx, elem := range s.jobPQ {
		if inserted {
			newQ[idx+1] = elem
		} else {
			if s.score(elem) > s.score(j) {
				newQ[idx] = j
				inserted = true
				newQ[idx+1] = elem
			} else {
				newQ[idx] = elem
			}
		}
	}
	if !inserted {
		newQ[len(s.jobPQ)] = j
	}
	s.jobPQ = newQ
}

// pop the first job from priority queue
func (s *server) popJob() *job {
	if len(s.jobPQ) > 1 {
		job := s.jobPQ[0]
		s.jobPQ = s.jobPQ[1:]
		return job
	} else if len(s.jobPQ) == 1 {
		job := s.jobPQ[0]
		s.jobPQ = nil
		return job
	} else {
		return nil
	}
}
