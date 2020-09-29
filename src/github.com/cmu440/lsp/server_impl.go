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
	"sort"
)

type server struct {
	udpAddr *lspnet.UDPAddr
	udpConn *lspnet.UDPConn
	// Stores a linked list of all clients, defined upon server creation
	clients []*clientInfo
	counter int // counter for creating connID
	// Channels for receiving messages
	incomeConn    chan *lspnet.UDPAddr
	incomeData    chan *Message
	incomeAck     chan *Message
	// Messages
	orderedData	[]*Message
	readChan chan *readMsg
	writeChan chan *writeMsg
}

// Stores all information of a client, defined when a connection is 
// established
type clientInfo struct {
	connID int
	cliAddr *lspnet.UDPAddr
	writeSeqNum int // Latest Sequence Number for writes
	readSeqNum int // Latest Sequence Number (ordered) for reads
	pendingData []*Message // Incoming unordered msgs
	pendingWrites []*Message // Unsent msgs (due to window size, etc)
	unackedWrites []*Message // Sent msgs but not acked by client
}

type readMsg struct {
	connID int
	payload []byte
	err error
}

type writeMsg struct {
	connID int
	payload []byte
	retChan chan error
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
	    incomeConn:     make(chan *lspnet.UDPAddr),
	    incomeData:     make(chan *Message),
	    incomeAck:      make(chan *Message),
	    readChan: 	    make(chan *readMsg),
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
	    	c := s.addClient(addr)
	    	// send ack msg
	    	ack := NewAck(c.connID, 0)
	    	s.writeToClient(c, ack)
	
	    // new data msgs
	    case msg  := <-s.incomeData:
	    	c := s.findClient(msg.ConnID)
	    	if c == nil {
	    		break
	    	}
	    	s.storeData(c, msg)
	    	// send ack msg
	    	ack := NewAck(c.connID, c.writeSeqNum + 1)
	    	c.writeSeqNum++
	    	s.writeToClient(c, ack)

	    	// send data to s.readChan once available
	    	go func() {
	    		if s.readChan == nil || len(s.orderedData) == 0 {
	    			return
	    		}
	    		if len(s.readChan) != 0 {
	    			<-s.readChan
	    		}
	    		data := s.orderedData[len(s.orderedData) - 1]
	    		var err error
	    		if data.Payload == nil {
	    			err = errors.New("client connection has been closed")
	    		} else {
	    			err = nil
	    		}
	    		s.readChan<- &readMsg {
	    			connID: data.ConnID,
	    			payload: data.Payload,
	    			err: err,
	    		}
	    		fmt.Println("finished read")
	    	} ()
	    	fmt.Println(ack)
	
	    // ack msgs & epoch msgs
	    case msg  := <-s.incomeAck:
	    	fmt.Println(msg)

	    case req  := <-s.writeChan:
	    	c := s.findClient(req.connID)
	    	if c == nil {
	    		req.retChan<- errors.New("client connection has been closed")
	    		break
	    	}
	    	req.retChan<- nil

	    	cs := s.CalculateCheckSum(
	    		c.connID, c.writeSeqNum + 1,len(req.payload), req.payload,
	    	)
	    	msg := NewData(
	    		c.connID, c.writeSeqNum + 1, len(req.payload), 
	    		req.payload, cs,
	    	)
	    	c.writeSeqNum++
	    	fmt.Println(msg.String())
	    	s.writeToClient(c, msg)
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
        fmt.Println(readMsg.String())
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
    if s.readChan == nil {
    	return 0, nil, errors.New("The server has been closed")
    }
    ret := <-s.readChan
    return ret.connID, ret.payload, ret.err
}

func (s *server) Write(connId int, payload []byte) error {
    errChan := make(chan error)
    fmt.Println("called write")
    s.writeChan<- &writeMsg {
    	connID: connId,
		payload: payload,
		retChan: errChan,
    }
    fmt.Println("finished write")

    return <-errChan
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

// Creates a new client and add to server's list; returns the new client
func (s *server) addClient(addr *lspnet.UDPAddr) *clientInfo {
	c := &clientInfo{
        connID: s.counter + 1,
        cliAddr: addr,
        writeSeqNum: 0,
        readSeqNum: 0,
    }

    s.clients = append(s.clients, c)
    s.counter++
    return c
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

func (s *server) writeToClient(c *clientInfo, msg *Message) error {
	b, marshalErr := json.Marshal(msg)
	if marshalErr != nil {
		return marshalErr
	}

	_, writeErr := s.udpConn.WriteToUDP(b, c.cliAddr)
	if writeErr != nil {
		return writeErr
	}
	return nil
}

// Stores input data msg in server if it is in order, otherwise stores in client,
// checks for remaining client stored msgs in client and move now ordered msgs to
// server
func (s *server) storeData(c *clientInfo, msg *Message) {
	// Msg in order, store in the list
	if msg.SeqNum == c.readSeqNum + 1 {
		s.orderedData = append(s.orderedData, msg)
		c.readSeqNum++

		var newData []*Message // stores remaining unordered msgs
		for _, d := range c.pendingData {
			if d.SeqNum == c.readSeqNum + 1 {
				s.orderedData = append(s.orderedData, d)
				c.readSeqNum++
			} else {
				newData = append(newData, d)
			}
		}
		c.pendingData = newData
	} else {
		c.pendingData = append(c.pendingData, msg)		
	}
	
	// sort unordered msgs
	sort.Slice(c.pendingData, func(i, j int) bool {
		return c.pendingData[i].SeqNum < c.pendingData[j].SeqNum
	})
	return
}

func (s *server) CalculateCheckSum(ID int, seqNum int, size int, payload []byte) uint16 {
    // var checksumTmp uint32
    // var mask uint32
    // mask = 0x0000ffff
    // checksumTmp = 0
    // checksumTmp += Int2Checksum(ID)
    // checksumTmp += Int2Checksum(seqNum)
    // checksumTmp += Int2Checksum(size)
    // checksumTmp += ByteArray2Checksum(payload)
    // for checksumTmp > 0xffff {
    //  curSum := checksumTmp >> 16
    //  remain := checksumTmp & mask
    //  checksumTmp = curSum + remain
    // }
    // return uint16(checksumTmp)

    // For testing with crunner_sol
    var mask uint32
    mask = 0x0000ffff
    checksumTmp := ByteArray2Checksum(payload)
    for checksumTmp > 0xffff {
        curSum := checksumTmp >> 16
        remain := checksumTmp & mask
        checksumTmp = curSum + remain
    }
    return uint16(checksumTmp)
}