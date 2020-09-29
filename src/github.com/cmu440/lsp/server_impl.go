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
)

type server struct {
	udpAddr *lspnet.UDPAddr
	udpConn *lspnet.UDPConn
	// Stores a linked list of all clients, defined upon server creation
	clients []*clientInfo
	counter int // counter for creating connID
	// Channels for receiving messages
	incomeData    chan *Message
	incomeAck     chan *Message
	incomeConn    chan *lspnet.UDPAddr
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
	    udpAddr:        addr,
	    udpConn:        serverConn,
	    clients:        nil,
	    counter:        0,
	}
	
	go s.MainRoutine()
	go s.ReadRoutine()
	
	return s, nil
}

// Processes all incoming messages
func (s *server) MainRoutine() {
	for {
	    select {
	    // new connections 
	    case addr := <-s.incomeConn:
	    	s.addClient(addr)
	
	    // new data msgs
	    case msg  := <-s.incomeData:
	    	fmt.Println(msg)
	
	    // ack msgs & epoch msgs
	    case msg  := <-s.incomeAck:
	    	fmt.Println(msg)
	    }
	}
}

// Reads from udp connection; determines the msg type and sends msg to 
// mainroutine for further processing
func (s *server) ReadRoutine() {
	for {
	    // Read from connection
	        buff := make([]byte, 2000)
	        len, cliAddr, err := s.udpConn.ReadFromUDP(buff[0:])
	        if err != nil {
	            return
	        }
	        
	        // Parse message
	        var readMsg Message
	        if json.Unmarshal(buff[0:len], &readMsg) != nil {
	            continue
	        }
	
	        // Check message type
	        switch readMsg.Type {
	            case MsgConnect:
	                s.incomeConn<- cliAddr
	
	            case MsgData:
	                s.incomeData<- &readMsg
	
	            case MsgAck:
	                // TODO: implement sliding window
	                s.incomeAck<- &readMsg
	        }
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

func (s *server) addClient(addr *lspnet.UDPAddr) {
	c := &clientInfo{
        connID: s.counter + 1,
        cliAddr: addr,
        currSN: 0,
    }

    s.clients = append(s.clients, c)
    s.counter++
}

func (s *server) findClient(connid int) *clientInfo {
    for _, c := range s.clients {
        if c.connID == connid {
            return c
        }
    }
    return nil
}

func (s *server) removeClient(connid int) *clientInfo {
    for i, c := range s.clients {
        if c.connID == connid {
            s.clients[i] = s.clients[len(s.clients) - 1]
            return c
        }
    }
    return nil
}