// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"sort"
	"strconv"
	"time"
)

const ClosedClientMsgSeqNum = -1 // used in a ack msg to indicate that the client has been closed

type server struct {
	udpAddr     *lspnet.UDPAddr
	udpConn     *lspnet.UDPConn
	clients     []*clientInfo // Stores a linked list of all clients, defined upon server creation
	counter     int           // counter for creating connID
	orderedData []*Message    // stores ordered data msgs that are ready to read
	params      *Params
	closed      bool // Indicate whether or not the server is closed
	ticker      *time.Ticker

	storeChan         chan *Message       // channel for communications between main and read routines of storing data msgs
	readRetChan       chan *Message       // channel for passing ready-to-read msgs to Read function
	readDoneChan      chan bool           // channel for signaling Read is done reading
	writeChan         chan *writeMsg      // channel for signaling a write request from the Write funciton
	ackChan           chan *Message       // channel for communications between main and read routines of sending ack msgs
	epochChan         <-chan time.Time    // channel for passing epoch events
	rfChan            chan *clientInfo    // channel for indicating a msg is received from a client this epoch
	stChan            chan *clientInfo    // channel for indicating a msg is sent to a client this epoch
	addCliChan        chan *addClientReq  // channel for adding a client to the server's list
	findCliChan       chan *findClientReq // channel for searching for a client
	closeConnChan     chan *closeConnReq  // channel for signaling a close conneciton request from the CloseConn funciton
	closeChan         chan chan error     // channel for signaling a close server request from the Close function
	closeErrChan      chan error          // channel for passing errors after Close() is called
	clientsClosedChan chan bool           // indicate all clients have been closed
}

// Stores all information of a client, defined when a connection is established
type clientInfo struct {
	connID        int
	cliAddr       *lspnet.UDPAddr
	writeSeqNum   int             // Latest Sequence Number for writes
	readSeqNum    int             // Latest Sequence Number (ordered) for reads
	pendingData   []*Message      // Incoming unordered msgs
	pendingWrites []*Message      // Unsent msgs (due to window size, etc)
	unackedWrites []*unackedWrite // Sent msgs but not acked by client
	receivedFrom  bool            // Indicate whether or not a msg is received from this client during the current epoch
	sentTo        bool            // Indicate whether or not a msg is sent to this client during the current epoch
	noMsgEpoch    int             // Epochs passed without a received msg
	closed        bool            // Indicate whether or not the client is closed
}

type writeMsg struct {
	connID  int
	payload []byte
	retChan chan error
}

type unackedWrite struct {
	currEpoch   int
	currBackoff int
	nextBackoff int
	msg         *Message
}

type addClientReq struct {
	retChan chan *clientInfo
	addr    *lspnet.UDPAddr
}

type findClientReq struct {
	retChan chan *clientInfo
	connID  int
}

type closeConnReq struct {
	retChan chan error
	connID  int
}

// Creates, initiates, and returns a new server
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
		udpAddr:           addr,
		udpConn:           serverConn,
		clients:           nil,
		counter:           0,
		orderedData:       nil,
		storeChan:         make(chan *Message),
		readRetChan:       make(chan *Message, 1), // use size 1 buffered channel to avoid blocking
		readDoneChan:      make(chan bool),
		writeChan:         make(chan *writeMsg),
		ackChan:           make(chan *Message),
		epochChan:         make(<-chan time.Time),
		rfChan:            make(chan *clientInfo),
		stChan:            make(chan *clientInfo),
		addCliChan:        make(chan *addClientReq),
		findCliChan:       make(chan *findClientReq),
		closeConnChan:     make(chan *closeConnReq),
		closeChan:         make(chan chan error),
		clientsClosedChan: make(chan bool),
		params:            params,
	}

	go s.mainRoutine()
	go s.readRoutine()

	return s, nil
}

// Reads a data message from a client and returns its payload, and the
// connection ID associated with the client that sent the message.
func (s *server) Read() (int, []byte, error) {
	if s.readRetChan == nil {
		return 0, nil, errors.New("The server has been closed")
	}

	msg := <-s.readRetChan
	s.readDoneChan <- true
	if msg.SeqNum == ClosedClientMsgSeqNum {
		return msg.ConnID, nil, errors.New("Client " + strconv.Itoa(msg.ConnID) + " has been closed")
	}
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

// Terminates the client with the specified connection ID
func (s *server) CloseConn(connId int) error {
	retChan := make(chan error)
	s.closeConnChan <- &closeConnReq{
		retChan: retChan,
		connID:  connId,
	}
	return <-retChan
}

// Terminates all currently connected clients and shuts down the LSP server
func (s *server) Close() error {
	errChan := make(chan error)

	s.closeChan <- errChan
	select {
	case <-s.clientsClosedChan:
		break
	case err := <-errChan:
		return err
	}

	s.udpConn.Close()
	return nil
}

// =============================   Go Routines    =============================
// Handles writes, epochs, and ack msgs; process operations on client list and all clients
func (s *server) mainRoutine() {
	// initialize timer
	s.ticker = time.NewTicker(time.Duration(s.params.EpochMillis) * time.Millisecond)
	s.epochChan = s.ticker.C

	for {
		select {
		case req := <-s.writeChan:
			s.writeReqHandler(req)

		case msg := <-s.ackChan:
			closeMain := s.ackMsgHandler(msg)
			if closeMain {
				s.clientsClosedChan <- true
				s.ticker.Stop()
				return
			}

		case <-s.epochChan:
			s.epochHandler()

		case c := <-s.rfChan:
			c.receivedFrom = true

		case c := <-s.stChan:
			c.sentTo = true

		case req := <-s.addCliChan:
			req.retChan <- s.addClient(req.addr)

		case req := <-s.findCliChan:
			c := s.findClient(req.connID)
			if c == nil && c.closed {
				req.retChan <- nil
			} else {
				req.retChan <- c
			}

		case req := <-s.closeConnChan:
			if s.closed {
				req.retChan <- errors.New("Server has been closed")
				break
			}

			c := s.findClient(req.connID)
			if c != nil {
				c.closed = true
				req.retChan <- nil
				if len(c.unackedWrites) == 0 {
					s.closeClient(c.connID)
					break
				}
			} else {
				req.retChan <- errors.New("This client does not exist")
			}

		case data := <-s.storeChan:
			s.orderedData = append(s.orderedData, data)
			s.prepareForRead()

		case <-s.readDoneChan:
			s.prepareForRead()

		case errChan := <-s.closeChan:
			if s.closed {
				errChan <- errors.New("Server has been closed")
				break
			}
			s.closed = true
			s.closeErrChan = errChan
			s.readRetChan = nil

			for _, c := range s.clients {
				if c != nil {
					c.closed = true
					if len(c.unackedWrites) == 0 {
						s.closeClient(c.connID)
					}
				}
			}
			if s.allClientsClosed() {
				s.clientsClosedChan <- true
				s.clientsClosedChan <- true
				s.ticker.Stop()
				return
			}
		}
	}
}

// Reads from udp connection; determines the msg type and sends msg to
// mainroutine for further processing if necessary
func (s *server) readRoutine() {
	for {
		// Read from connection
		buff := make([]byte, MaxMessageByteLen)
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
			retChan := make(chan *clientInfo)
			s.addCliChan <- &addClientReq{retChan: retChan, addr: addr}
			c := <-retChan
			close(retChan)

			s.rfChan <- c
			// send ack msg
			ack := NewAck(c.connID, 0)
			s.writeToClient(c, ack)
			s.stChan <- c

		case MsgData:
			if !checkCorrect(&msg) {
				break
			}
			retChan := make(chan *clientInfo)
			s.findCliChan <- &findClientReq{retChan: retChan, connID: msg.ConnID}
			c := <-retChan
			close(retChan)

			if c == nil {
				break
			}
			// send ack msg
			ack := NewAck(c.connID, msg.SeqNum)
			s.writeToClient(c, ack)

			s.storeData(c, &msg)
			s.rfChan <- c
			s.stChan <- c

		case MsgAck:
			s.ackChan <- &msg
		}
	}
}

// =============================     Handlers     =============================
// Handles Write function requests, check if the msg fits within the sliding window,
// if so, send to client, otherwise store in pendingWrites
func (s *server) writeReqHandler(req *writeMsg) {
	if s.closed {
		req.retChan <- errors.New("Server has been closed")
		return
	}

	c := s.findClient(req.connID)
	if c == nil || c.closed {
		req.retChan <- errors.New("client connection has been closed")
		return
	}

	req.retChan <- nil
	cs := message2CheckSum(
		c.connID, c.writeSeqNum+1, len(req.payload), req.payload,
	)

	msg := NewData(
		c.connID, c.writeSeqNum+1, len(req.payload),
		req.payload, cs,
	)

	c.writeSeqNum++
	if s.canSend(c, msg) {
		s.writeToClient(c, msg)
		c.sentTo = true
		c.unackedWrites = append(c.unackedWrites, &unackedWrite{
			currEpoch:   0,
			currBackoff: 0,
			nextBackoff: 0,
			msg:         msg,
		})
	} else {
		c.pendingWrites = append(c.pendingWrites, msg)
	}
}

// Handles ack msg, returns true if the server is closed and the main routine can be safely closed
func (s *server) ackMsgHandler(msg *Message) bool {
	c := s.findClient(msg.ConnID)
	if c != nil {
		c.receivedFrom = true
		// check if ack matches any unacked msgs
		for i, m := range c.unackedWrites {
			if m.msg.SeqNum == msg.SeqNum {
				// remove the unacked msg from the list
				copy(c.unackedWrites[i:], c.unackedWrites[i+1:])
				c.unackedWrites = c.unackedWrites[:len(c.unackedWrites)-1]

				var newPendingWrites []*Message
				// add a pending msg to window if possible
				for _, m := range c.pendingWrites {
					if s.canSend(c, m) {
						s.writeToClient(c, m)
						c.sentTo = true
						c.unackedWrites = append(c.unackedWrites, &unackedWrite{
							currEpoch:   0,
							currBackoff: 0,
							nextBackoff: 0,
							msg:         m,
						})
					} else {
						newPendingWrites = append(newPendingWrites, m)
					}
				}
				c.pendingWrites = newPendingWrites

				// last ack received and pending writes handled, remove client
				if c.closed && len(c.unackedWrites) == 0 {
					s.removeClient(c.connID)
					// last client removed, close routine and stop the timer
					if s.closed && s.allClientsClosed() {
						return true
					}
					s.orderedData = append(s.orderedData, NewAck(c.connID, ClosedClientMsgSeqNum))
					s.prepareForRead()
					break
				}
				break
			}
		}
	}
	return false
}

// Handles epoch events; check lost clients, resend unacked messages, and send heartbeat
func (s *server) epochHandler() {
	for _, client := range s.clients {
		if client == nil { // client closed
			continue
		}
		if !client.receivedFrom {
			client.noMsgEpoch++
		} else {
			client.noMsgEpoch = 0
		}

		// connection lost, close client
		if client.noMsgEpoch > s.params.EpochLimit {
			s.closeClient(client.connID)
			if s.closed && s.closeErrChan != nil {
				s.closeErrChan <- errors.New("a client has been lost")
			}
			continue
		}

		// check msg and see if resend is necessary
		for _, m := range client.unackedWrites {
			if m.currBackoff == 0 {
				if m.nextBackoff == 0 {
					m.nextBackoff = 1
				} else {
					m.nextBackoff *= 2
				}
				if m.nextBackoff > s.params.MaxBackOffInterval {
					m.nextBackoff = s.params.MaxBackOffInterval
				}

				m.currBackoff = m.nextBackoff
				s.writeToClient(client, m.msg)
				client.sentTo = true
			} else {
				m.currBackoff--
			}
		}

		// check if heartbeat is necessary
		if !client.sentTo && !client.closed {
			heartbeat := NewAck(client.connID, 0)
			s.writeToClient(client, heartbeat)
		}

		// reset sentTo and readFrom for next epoch
		client.receivedFrom = false
		client.sentTo = false
	}
}

// ============================= Helper Functions =============================
// Creates a new client and add to server's list; returns the new client
func (s *server) addClient(addr *lspnet.UDPAddr) *clientInfo {
	for _, c := range s.clients {
		if c != nil && c.cliAddr == addr {
			return nil
		}
	}
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
		if c != nil && c.connID == connid {
			return c
		}
	}
	return nil
}

func (s *server) removeClient(connid int) *clientInfo {
	for i, c := range s.clients {
		if c != nil && c.connID == connid {
			s.clients[i] = nil
			return c
		}
	}
	return nil
}

func (s *server) closeClient(connid int) {
	s.removeClient(connid)
	s.orderedData = append(s.orderedData, NewAck(connid, ClosedClientMsgSeqNum))
	s.prepareForRead()
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

func (s *server) allClientsClosed() bool {
	for _, c := range s.clients {
		if c != nil {
			return false
		}
	}
	return true
}

// Inserts the first element of the ordered msg list to the read return channel if the
// channel is empty
func (s *server) prepareForRead() {
	if len(s.orderedData) > 0 && s.readRetChan != nil && len(s.readRetChan) == 0 {
		// Pop the first elem out and send through channel
		s.readRetChan <- s.orderedData[0]

		if len(s.orderedData) > 1 {
			s.orderedData = s.orderedData[1:]
		} else {
			s.orderedData = nil
		}
	}
}

// Stores input data msg in server if it is in order, otherwise stores in client, checks
// for remaining client stored msgs in client and move now ordered msgs to server
func (s *server) storeData(c *clientInfo, msg *Message) {
	// Msg in order, store in the list
	if msg.SeqNum == c.readSeqNum+1 {
		s.storeChan <- msg
		c.readSeqNum++

		var newData []*Message // stores remaining unordered msgs
		for _, d := range c.pendingData {
			if d.SeqNum == c.readSeqNum+1 {
				s.storeChan <- d
				c.readSeqNum++
			} else {
				newData = append(newData, d)
			}
		}
		c.pendingData = newData
	} else if msg.SeqNum > c.readSeqNum+1 {
		for _, d := range c.pendingData {
			// duplicate msg
			if msg.SeqNum == d.SeqNum {
				return
			}
		}
		c.pendingData = append(c.pendingData, msg)
	} else { // msg.SeqNum < c.readSeqNum + 1
		return
	}

	// sort unordered msgs
	sort.Slice(c.pendingData, func(i, j int) bool {
		return c.pendingData[i].SeqNum < c.pendingData[j].SeqNum
	})
}

func (s *server) canSend(c *clientInfo, msg *Message) bool {
	return len(c.unackedWrites) < s.params.MaxUnackedMessages &&
		(len(c.unackedWrites) == 0 ||
			msg.SeqNum < c.unackedWrites[0].msg.SeqNum+s.params.WindowSize) // elements in unackedWrites are appended in order, so the first one is the oldest
}