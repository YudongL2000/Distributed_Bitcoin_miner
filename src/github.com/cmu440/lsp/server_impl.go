// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"sort"
	"strconv"
)

type server struct {
	udpAddr *lspnet.UDPAddr
	udpConn *lspnet.UDPConn
	// Stores a linked list of all clients, defined upon server creation
	clients []*clientInfo
	counter int // counter for creating connID
	// Messages
	orderedData []*Message
	readChan    chan *Message
	writeChan   chan *writeMsg
}

// Stores all information of a client, defined when a connection is
// established
type clientInfo struct {
	connID        int
	cliAddr       *lspnet.UDPAddr
	writeSeqNum   int        // Latest Sequence Number for writes
	readSeqNum    int        // Latest Sequence Number (ordered) for reads
	pendingData   []*Message // Incoming unordered msgs
	pendingWrites []*Message // Unsent msgs (due to window size, etc)
	unackedWrites []*Message // Sent msgs but not acked by client
}

type writeMsg struct {
	connID  int
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
	addr, resErr := lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	if resErr != nil {
		return nil, resErr
	}
	serverConn, listErr := lspnet.ListenUDP("udp", addr)
	if listErr != nil {
		return nil, listErr
	}

	s := &server{
		udpAddr:     addr,
		udpConn:     serverConn,
		clients:     nil,
		counter:     0,
		orderedData: nil,
		readChan:    make(chan *Message, 20),
		writeChan:   make(chan *writeMsg),
	}

	go s.MainRoutine()
	go s.ReadRoutine()

	return s, nil
}

// Processes all incoming messages
func (s *server) MainRoutine() {
	for {
		select {
		case req := <-s.writeChan:
			c := s.findClient(req.connID)
			if c == nil {
				req.retChan <- errors.New("client connection has been closed")
				break
			}
			req.retChan <- nil

			cs := s.CalculateCheckSum(
				c.connID, c.writeSeqNum+1, len(req.payload), req.payload,
			)
			msg := NewData(
				c.connID, c.writeSeqNum+1, len(req.payload),
				req.payload, cs,
			)
			c.writeSeqNum++
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
		var msg Message
		if json.Unmarshal(buff[0:len], &msg) != nil {
			continue
		}
		// Check message type
		switch msg.Type {
		case MsgConnect:
			addr := cliAddr
			c := s.addClient(addr)
			// send ack msg
			ack := NewAck(c.connID, 0)
			s.writeToClient(c, ack)

		case MsgData:
			c := s.findClient(msg.ConnID)
			if c == nil {
				break
			}
			s.storeData(c, &msg)
			// send ack msg
			ack := NewAck(c.connID, msg.SeqNum)
			s.writeToClient(c, ack)

		case MsgAck:
			// TODO: implement sliding window
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	if s.readChan == nil {
		return 0, nil, errors.New("The server has been closed")
	}
	msg := <-s.readChan
	return msg.ConnID, msg.Payload, nil
}

func (s *server) Write(connId int, payload []byte) error {
	errChan := make(chan error)
	s.writeChan <- &writeMsg{
		connID:  connId,
		payload: payload,
		retChan: errChan,
	}

	return <-errChan
}

func (s *server) CloseConn(connId int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}

// ============================= Helper Functions =============================
// Creates a new client and add to server's list; returns the new client
func (s *server) addClient(addr *lspnet.UDPAddr) *clientInfo {
	c := &clientInfo{
		connID:      s.counter + 1,
		cliAddr:     addr,
		writeSeqNum: 0,
		readSeqNum:  0,
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
			s.clients[i] = s.clients[len(s.clients)-1]
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
	if msg.SeqNum == c.readSeqNum+1 {
		s.orderedData = append(s.orderedData, msg)
		c.readSeqNum++

		var newData []*Message // stores remaining unordered msgs
		for _, d := range c.pendingData {
			if d.SeqNum == c.readSeqNum+1 {
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

	data := s.orderedData
	s.orderedData = nil
	for _, d := range data {
		s.readChan <- d
	}
	return
}

func (s *server) CalculateCheckSum(ID int, seqNum int, size int, payload []byte) uint16 {
	var checksumTmp uint32
	var mask uint32
	mask = 0x0000ffff
	checksumTmp = 0
	checksumTmp += Int2Checksum(ID)
	checksumTmp += Int2Checksum(seqNum)
	checksumTmp += Int2Checksum(size)
	checksumTmp += ByteArray2Checksum(payload)
	for checksumTmp > 0xffff {
		curSum := checksumTmp >> 16
		remain := checksumTmp & mask
		checksumTmp = curSum + remain
	}
	return uint16(checksumTmp)
}
