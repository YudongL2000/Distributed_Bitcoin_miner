// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"github.com/cmu440/lspnet"
	"fmt"
	"encoding/json"
	"bytes"
	"log"
	"strconv"
	"container/list"
)

type server struct {
	udpAddr *lspnet.UDPAddr
	// Stores a linked list of all clients, defined upon server creation
	clientList *list.List
	readChan chan chan *readReqMsg // used for Read() requests
	writeChan chan *writeReqMsg // used for Write() requests
	storeDataChan chan *Message // used to store data sent by clients
	storedData *list.List
	counter int // counter for creating connID
}

// Stores all information of a client, defined when a connection is 
// established
type clientInfo struct {
	connID int
	conn *lspnet.UDPConn
	readMsgChan chan *Message
	writeMsgChan chan *Message
	writeReqChan chan *writeReqMsg // Used for Write() requests
	pendingWrites *list.List // List of Messages pending to be written
	unackedWrites *list.List // List of sent but unacked Messages
	unorderedReads *list.List // List of unordered read Messages
	lastReadSN int // The seq num of the last in order Message
	currSN int // Current Sequence Number
}

type readReqMsg struct {
	connID int
	payload []byte
	err error
}

type writeReqMsg struct {
	returnChan chan error
	connID int
	payload []byte
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", ":" + strconv.Itoa(port))
	s := &server{
		udpAddr: 		addr,
		clientList: 	list.New(),
		readChan:   	make(chan chan *readReqMsg),
		writeChan:  	make(chan *writeReqMsg),
		storeDataChan:  make(chan *Message),
		storedData: 	list.New(),
		counter:    	0,
	}

	go s.MainRoutine()

	return s, err
}

// Stores all data coming from clients, process write / read requests
func (s *server) MainRoutine() {
	go s.Start()

	for {
		select {
		case returnChan := <-s.readChan:
			var msg Message
			if s.storedData.Len() == 0 {
				select {
				case msg := <-s.storeDataChan:
					break

				// TODO: close case
				}				
			}
			s.storedData.PushBack(msg)
			msg = s.storedData.Remove(s.storedData.Front()).(Message)
			if msg.Payload == nil {
				returnChan<- &readReqMsg{
					connID: msg.ConnID, 
					payload: nil, 
					err: errors.New("client has disconnected")}
			}
			returnChan<- &readReqMsg{connID: msg.ConnID, payload: msg.Payload, err: nil}
		
		case write := <-s.writeChan:
			sent := false
			for e := s.clientList.Front(); e != nil; e = e.Next() {
				c := e.Value.(clientInfo)
				if write.connID == c.connID {
					c.writeReqChan<- write
					break
				}
			}
			if !sent {
				write.returnChan<- errors.New("connection closed")
			} else {
				write.returnChan<- nil
			}
		
		case msg := <-s.storeDataChan:
			s.storedData.PushBack(msg)

		// TODO: close case
		}
	}
}

// Listens for any incoming connections
func (s *server) Start() {
	for {
		conn, err := lspnet.ListenUDP("udp", s.udpAddr)
		if err != nil {
			break
		}

		c := &clientInfo{
			connID: s.counter + 1,
			conn: conn,
			readMsgChan: make(chan *Message),
			writeMsgChan: make(chan *Message),
			pendingWrites: list.New(),
			unackedWrites: list.New(),
			unorderedReads: list.New(),
			lastReadSN: 0,
			currSN: 0,
		}
		s.clientList.PushBack(c)
		s.counter++

		go s.ClientRoutine(c)
	}
}

func (s *server) ClientRoutine(c *clientInfo) {
	go s.ReadRoutine(c)
	go s.WriteRoutine(c)
	for {
		select {
		case readMsg := <-c.readMsgChan:
			switch readMsg.Type {
				case MsgConnect:
					msg := NewAck(c.connID, c.currSN)
					c.currSN++
					c.writeMsgChan<- msg
				case MsgData:
					c.unorderedReads.PushFront(readMsg)
					// check for in order reads
					for e := c.unorderedReads.Front(); e != nil; e = e.Next() {
						if e.Value.(Message).SeqNum == c.lastReadSN + 1 {
							next := e.Next()
							msg := c.unorderedReads.Remove(e).(Message)
							s.storeDataChan<- &msg
							c.writeMsgChan<- NewAck(c.connID, msg.SeqNum)
							c.lastReadSN++
							e = next
						}
					}
				case MsgAck:
					// TODO: implement sliding window
				default:
					// Shouldn't happen
					continue
			}

		case write := <-c.writeReqChan:
			// TODO: implement sliding window
			msg := NewData(
				c.connID, 
				c.currSN, 
				len(write.payload), 
				write.payload,
				checkSum(c.connID, c.currSN, write.payload, len(write.payload)),
			)
			c.currSN++
			c.writeMsgChan<- msg

		// TODO: close case, send msg with nil payload to mainroutine
		}
	}
}

func (s *server) ReadRoutine(c *clientInfo) {
	for {
		select {
		default:
			buff := make([]byte, 2000)
			len, cliAddr, err := c.conn.ReadFromUDP(buff[0:])

			var msg Message
			if json.Unmarshal(buff[0:len], &msg) != nil {
				continue
			}
			c.readMsgChan <- &msg

		// TODO: close case
		}
	}
}

func (s *server) WriteRoutine(c *clientInfo) {
	for {
		select {
		case writeMsg := <-c.writeMsgChan:
			b, err := json.Marshal(writeMsg)
			if err != nil {
				break
			}

			_, writeErr := c.conn.Write(b)
			if writeErr != nil {
				break
			}

		// TODO: close case
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	returnChan := make(chan *readReqMsg)
	s.readChan<- returnChan
	msg := <-returnChan
	return msg.connID, msg.payload, msg.err
}

func (s *server) Write(connId int, payload []byte) error {
	errorChan := make(chan error)
	s.writeChan<- &writeReqMsg{returnChan: errorChan, connID: connId, payload: payload}
	err := <-errorChan
	return err
}

func (s *server) CloseConn(connId int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}


// ============================= Helper Functions =============================
func checkSum(connID int, seqNum int, payload []byte, size int) uint16 {
	// TODO: implement me
	return uint16(0)
}
