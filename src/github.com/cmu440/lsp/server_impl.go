// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"github.com/cmu440/lspnet"
	"fmt"
	"encoding/json"
	//"bytes"
	//"log"
	"strconv"
	"container/list"
)

type server struct {
	udpAddr *lspnet.UDPAddr
	udpConn *lspnet.UDPConn
	// Stores a linked list of all clients, defined upon server creation
	clientList *list.List
	counter int // counter for creating connID
}

// Stores all information of a client, defined when a connection is 
// established
type clientInfo struct {
	connID int
	cliAddr *lspnet.UDPAddr
	currSN int // Current Sequence Number
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, resErr := lspnet.ResolveUDPAddr("udp", ":" + strconv.Itoa(port))
	if resErr != nil {
		return nil, resErr
	}
	serverConn, listErr := lspnet.ListenUDP("udp", addr)
	if listErr != nil {
		return nil, listErr
	}

	s := &server{
		udpAddr: 		addr,
		udpConn: 		serverConn,
		clientList: 	list.New(),
		counter:    	0,
	}

	go s.MainRoutine()
	go s.ReadRoutine()

	return s, nil
}

func (s *server) MainRoutine() {
		// // Check message type
		// 	switch readMsg.Type {
		// 		case MsgConnect:
		// 			

		// 		case MsgData:
		// 			c.unorderedReads.PushFront(readMsg)
		// 			// check for in order reads
		// 			for e := c.unorderedReads.Front(); e != nil; e = e.Next() {
		// 				if e.Value.(Message).SeqNum == c.lastReadSN + 1 {
		// 					next := e.Next()
		// 					msg := c.unorderedReads.Remove(e).(Message)
		// 					s.storeDataChan<- &msg
		// 					c.writeMsgChan<- NewAck(c.connID, msg.SeqNum)
		// 					c.lastReadSN++
		// 					e = next
		// 				}
		// 			}

		// 		case MsgAck:
		// 			// TODO: implement sliding window
		// 		default:
		// 			// Shouldn't happen
		// 			continue
		//	}

}

func (s *server) ReadRoutine() {
	for {
		// Read from connection
			buff := make([]byte, 2000)
			len, cliAddr, err := s.udpConn.ReadFromUDP(buff[0:])
			if err != nil {
				continue
			}
			
			// Parse message
			var readMsg Message
			if json.Unmarshal(buff[0:len], &readMsg) != nil {
				continue
			}

			// Check for existing clients
			found := false
			client := s.findClient(cliAddr)
			if client != nil {
				found = true
			}

			if !found {
				c := clientInfo{
					connID: s.counter + 1,
					cliAddr: cliAddr,
					currSN: 0,
				}
				s.clientList.PushBack(c)
				s.counter++
			}
			fmt.Println(cliAddr)
			fmt.Println(found)
			fmt.Println(readMsg.String())
	}
}

func (s *server) Read() (int, []byte, error) {
	select {} // Blocks indefinitely.
	return -1, nil, errors.New("not yet implemented")
}

func (s *server) Write(connId int, payload []byte) error {
	return errors.New("not yet implemented")
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

func (s *server) findClient(addr *lspnet.UDPAddr) *list.Element {
	for e := s.clientList.Front(); e != nil; e = e.Next() {
		if e.Value.(clientInfo).cliAddr.String() == addr.String() {
			return e
		}
	}
	return nil
}
