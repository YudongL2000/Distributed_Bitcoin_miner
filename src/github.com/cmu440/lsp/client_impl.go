// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"time"
)

const MaxMessageByteLen = 2000

//an element in the window created from writer payload
type sentQueueElem struct {
	acked       bool     //whether the message is acknowledged
	seqNum      int      //seq num of the message
	backOff     int      //current backoff needed for the next resend
	epochPassed int      //number of epochs after the last resend
	msg         *Message //message to send
}

type client struct {
	params            *Params
	connID            int //connection id
	connection        *lspnet.UDPConn
	connectionAck     chan bool //routine send back whether connection is acked
	seqNum            int
	wantedMsg         int                 //seqnum of next message to Read
	ackConn           int                 //indicate whether connection is already established
	readResponse      chan *Message       //Read function takes wanted message from this channel
	readEmpty         chan bool           //indicate if readResponse is empty
	window            []*sentQueueElem    //window
	windowStart       int                 //start of window , -1 if window empty
	windowUnAcked     int                 //unacked messages in the window
	unsentBuffer      []*sentQueueElem    //pending messages outside window, waiting to be sent
	recieveList       *list               //double linked list of received messages
	connMain          chan int            //the main channel send connid to other routines
	outgoPayload      chan []byte         //payload to be sent
	newWindowElem     chan *sentQueueElem //new window element created by main rountine
	newAckSeqNum      chan int            //the seqnum of new ack received by read Routine
	writeReq          chan bool           //write Request to the server by Write function
	writeReturn       chan bool           //confirmation for writing, if false, then write an error
	incomeData        chan *Message       //new MsgData received by the read Routine
	alreadyDisconnect bool                //the server is dropped
	readRountineQuit  chan bool           //close read Routine
	mainQuit          chan bool           //close main rountine
	quitReqTime       chan bool           //send close request to time Rountine, waiting for all messages sent and acked
	quitConfirm       chan bool           //all pending messages sent and acked , close available
	disconnect        chan bool           //the client disconnected from server after time out
	timerDrop         chan bool           //close TimeAndSend Rountine
	closeFinish       chan bool           //all pending messages processed, Close function could return
	serverAlive       chan bool           //received something from server, server connection is confirmed
	connIDReq         chan bool           //ConnID function called
	connIDAnswer      chan int            //send connection ID back to ConnID function
	closeCalled       bool                //close has already been called, no longer acept Read, Write and Close
}

//===============================================  Start of client_api functions =================================

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	conn, connError := lspnet.DialUDP("udp", nil, addr)
	if connError != nil {
		return nil, connError
	}
	c := &client{
		params:            params,
		connID:            -1,
		connection:        conn,
		connectionAck:     make(chan bool),
		seqNum:            1,
		wantedMsg:         1,
		ackConn:           0,
		readResponse:      make(chan *Message, 1),
		readEmpty:         make(chan bool),
		window:            nil,
		windowStart:       0,
		windowUnAcked:     0,
		unsentBuffer:      nil,
		recieveList:       newList(),
		connMain:          make(chan int),
		outgoPayload:      make(chan []byte),
		newWindowElem:     make(chan *sentQueueElem),
		newAckSeqNum:      make(chan int),
		writeReq:          make(chan bool),
		writeReturn:       make(chan bool),
		incomeData:        make(chan *Message),
		alreadyDisconnect: false,
		readRountineQuit:  make(chan bool),
		mainQuit:          make(chan bool),
		quitReqTime:       make(chan bool),
		quitConfirm:       make(chan bool),
		disconnect:        make(chan bool),
		timerDrop:         make(chan bool),
		closeFinish:       make(chan bool),
		serverAlive:       make(chan bool),
		connIDReq:         make(chan bool),
		connIDAnswer:      make(chan int),
		closeCalled:       false,
	}

	go c.readRoutine()
	go c.mainRoutine()
	go c.timeAndSendRountine()
	connectedInfo := <-c.connectionAck
	if connectedInfo == false {
		c.Close()
		return nil, errors.New("connection failed")
	}
	return c, nil
}

func (c *client) ConnID() int {
	c.connIDReq <- true
	answer := <-c.connIDAnswer
	return answer
}

func (c *client) Read() ([]byte, error) {
	msg := <-c.readResponse
	c.readEmpty <- true
	if msg == nil {
		return nil, errors.New("read request denied, connection failed")
	}
	return msg.Payload, nil
}

func (c *client) Write(payload []byte) error {
	c.writeReq <- true
	clientStatus := <-c.writeReturn
	if clientStatus {
		c.outgoPayload <- payload
		return nil
	} else {
		return errors.New("write Request Denied: client disconnected")
	}
}

func (c *client) Close() error {
	c.mainQuit <- true
	<-c.closeFinish
	return nil
}

//=============================================== End of client_api functions =============================

//============================================ Start of generic Helper functions =======================
//send a heart beat to server
func (c *client) sendConnAck(id int) {
	msg := NewAck(id, 0)
	c.writeMsg(msg)
}

//calculate the min of 2 integers
func minHelper(a int, b int) int {
	if a >= b {
		return b
	} else {
		return a
	}
}

//write a message to the server through connection
func (c *client) writeMsg(msg *Message) error {
	content, err := json.Marshal(msg)
	if err != nil {
		return err
	} else {
		_, writeErr := c.connection.Write(content)
		if writeErr != nil {
			return writeErr
		}
		return nil
	}
}

//store the message information in a double linked list
func (c *client) storeData(msg *Message) {
	listInsert(msg, c.recieveList)
}

//Terminate all go rountines and disconnect from the server
func (c *client) abortAll() {
	c.connection.Close()
	c.timerDrop <- true
	c.readRountineQuit <- true
	c.closeFinish <- true
	return
}

//============================================= End of Generic Helper Functions ==============================

//================================================= Start of window functions ================================

//if there's extra space in the window, we move as many window elements as
//we can from the unsent buffer
func (c *client) sendAllAvailable() bool {
	if len(c.unsentBuffer) == 0 {
		return false
	}
	firstUnsent := c.unsentBuffer[0].seqNum
	if len(c.window) == 0 {
		numSent := minHelper(c.params.MaxUnackedMessages, len(c.unsentBuffer))
		c.windowStart = firstUnsent
		c.windowUnAcked = numSent
		c.window = c.unsentBuffer[:numSent]
		for _, elem := range c.window {
			c.writeMsg(elem.msg)
		}
		c.unsentBuffer = c.unsentBuffer[numSent:]
		return true
	} else {
		remain := minHelper(c.params.WindowSize-(firstUnsent-c.windowStart), len(c.unsentBuffer))
		remain = minHelper(remain, c.params.MaxUnackedMessages-c.windowUnAcked)
		if remain <= 0 {
			return false
		} else {
			newSent := c.unsentBuffer[:remain]
			c.window = append(c.window, newSent...)
			c.windowUnAcked += remain
			for _, elem := range newSent {
				c.writeMsg(elem.msg)
			}
			c.unsentBuffer = c.unsentBuffer[remain:]
			return true
		}
	}
}

//if the first few messages in the window is acked, then we need to shift
//the window backwards
func (c *client) removeFinished() {
	counter := 0
	for _, elem := range c.window {
		if elem.acked {
			counter += 1
		} else {
			break
		}
	}
	c.window = c.window[counter:]
	if len(c.window) == 0 {
		c.windowStart = -1
	} else {
		c.windowStart = c.window[0].seqNum
	}
}

// after receiving ack, then we mark these windows that contains the corresponding
//messages as acked
func (c *client) processAck(sn int) bool {
	if (c.windowStart >= 0) && (c.windowStart <= sn) {
		idx := sn - c.windowStart
		if c.window[idx].acked == false {
			//c.window[idx].sentSuccess <- true
			c.window[idx].acked = true
			c.windowUnAcked = c.windowUnAcked - 1
			if idx == 0 {
				c.removeFinished()
			}
			res := c.sendAllAvailable()
			return res
		} else {
			return false
		}
	} else {
		return false
	}
}

//immediately send once after sending
func (c *client) ackNewWindowElem(newElem *sentQueueElem) {
	if len(c.window) == 0 {
		c.window = append(c.window, newElem)
		c.windowStart = newElem.seqNum
		c.windowUnAcked = 1
		c.writeMsg(newElem.msg)
	} else if (newElem.seqNum-c.window[0].seqNum < c.params.WindowSize) && (c.windowUnAcked < c.params.MaxUnackedMessages) {
		c.window = append(c.window, newElem)
		c.windowUnAcked += 1
		c.writeMsg(newElem.msg)
	} else {
		c.unsentBuffer = append(c.unsentBuffer, newElem)
	}
}

// add to epoch counter in all window elements
//resend if backoff is reached
func (c *client) updateWindow() bool {
	sendSth := false
	for _, element := range c.window {
		if element.acked == false {
			if element.epochPassed >= element.backOff {
				c.writeMsg(element.msg)
				element.epochPassed = 0
				if element.backOff == 0 {
					element.backOff += 1
				} else {
					element.backOff = element.backOff * 2
				}
				if element.backOff > c.params.MaxBackOffInterval {
					element.backOff = c.params.MaxBackOffInterval
				}
				sendSth = true
			} else {
				element.epochPassed += 1
			}
		}
	}
	return sendSth
}

//=========================================== End of window functions =====================================

//=========================================== Start of Rountines ==========================================
func (c *client) timeAndSendRountine() {
	heartBeatTimer := time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
	dropConnTimer := time.NewTicker(time.Duration(c.params.EpochLimit*c.params.EpochMillis) * time.Millisecond)
	aliveConfirm := false
	connConfirm := false
	disconnected := false
	wantQuit := false
	id := -1
	newConnElem := &sentQueueElem{
		acked:   false,
		seqNum:  0,
		backOff: 0,
		msg:     NewConnect(),
	}
	c.ackNewWindowElem(newConnElem)
	for {
		select {
		case <-heartBeatTimer.C:
			//refresh epoch timer
			heartBeatTimer = time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
			if disconnected {
				continue
			}
			resendMsg := c.updateWindow()
			// see if new window messsages are sent window
			aliveConfirm = resendMsg || aliveConfirm
			if aliveConfirm == false && connConfirm {
				c.sendConnAck(id)
				// no message sent in the last epoch, send a heart beat indicating connection
			}
			aliveConfirm = false
		case <-dropConnTimer.C:
			// Max epochs passed, no messages received
			if connConfirm == false {
				c.connectionAck <- false
				disconnected = true
				c.disconnect <- true
				continue
			} else {
				c.disconnect <- true
				disconnected = true
				if wantQuit {
					c.quitConfirm <- true
				}
			}
		case <-c.timerDrop:
			//quit rountine
			return
		case <-c.serverAlive:
			//received something from the server, reset the timeout Timer
			dropConnTimer = time.NewTicker(time.Duration(c.params.EpochLimit*c.params.EpochMillis) * time.Millisecond)
		case newElem := <-c.newWindowElem: // add new element to the window
			c.ackNewWindowElem(newElem)
			aliveConfirm = true
		case sn := <-c.newAckSeqNum:
			//new ack recieved
			// see if new  messsages are placed into the window and sent
			sendNew := c.processAck(sn)
			aliveConfirm = aliveConfirm || sendNew
			if sn == 0 {
				if connConfirm == false {
					//connection ack
					connConfirm = true
					c.connectionAck <- true
					id = <-c.connIDAnswer
				}
				continue
			}
			if wantQuit && (len(c.window) == 0) && (len(c.unsentBuffer) == 0) {
				//all messages sent out, ready to close client
				c.quitConfirm <- true
			}
		case <-c.quitReqTime:
			wantQuit = true
			if disconnected {
				c.quitConfirm <- true
			} else if wantQuit && (len(c.window) == 0) && (len(c.unsentBuffer) == 0) {
				//all messages sent out, ready to close client
				c.quitConfirm <- true
			}
		}
	}
}

//Main Rountine of the client
func (c *client) mainRoutine() {
	for {
		select {
		case <-c.mainQuit:
			//Close called
			if c.closeCalled == false {
				c.quitReqTime <- true
				c.closeCalled = true
			}
		case <-c.quitConfirm:
			//all pending messages sent out, ready for close
			c.alreadyDisconnect = true
			c.abortAll()
			return
		case <-c.disconnect:
			//connection timed out from server
			c.alreadyDisconnect = true
			addTerminatingElem(c.recieveList)
			if (len(c.readResponse) == 0) && (c.recieveList.head.seqNum == -1) {
				c.readResponse <- nil
			}
		case <-c.readEmpty:
			//last read function finished, buffered channel empty
			if c.closeCalled {
				continue
			}
			if len(c.readResponse) == 0 {
				if (!empty(c.recieveList)) && (c.recieveList.head.seqNum == c.wantedMsg) {
					readRes := sliceHead(c.recieveList)
					c.readResponse <- readRes
					c.wantedMsg += 1
				} else if (!empty(c.recieveList)) && (c.recieveList.head.seqNum == -1) {
					//Read should send out error message
					c.readResponse <- nil
				}
			}
		case ID := <-c.connMain:
			//connection established
			if c.ackConn == 0 {
				c.connID = ID
				c.ackConn = 1
				c.connIDAnswer <- c.connID
			}
		case msg := <-c.incomeData:
			//new data message received
			if c.closeCalled {
				continue
			}
			if msg.SeqNum >= c.wantedMsg {
				c.storeData(msg)
				if len(c.readResponse) == 0 {
					if (!empty(c.recieveList)) && (c.recieveList.head.seqNum == c.wantedMsg) {
						readRes := sliceHead(c.recieveList)
						c.readResponse <- readRes
						c.wantedMsg += 1
					} else if (!empty(c.recieveList)) && (c.recieveList.head.seqNum == -1) {
						c.readResponse <- nil
					}
				}
			}
		case payload := <-c.outgoPayload:
			//new payload to be sent
			if c.closeCalled {
				continue
			}
			payloadSize := len(payload)
			checkSum := message2CheckSum(c.connID, c.seqNum, payloadSize, payload)
			m := NewData(c.connID, c.seqNum, payloadSize, payload, checkSum)
			c.seqNum += 1
			newElem := &sentQueueElem{
				acked:       false,
				seqNum:      m.SeqNum,
				backOff:     0,
				epochPassed: 0,
				msg:         m,
			}
			c.newWindowElem <- newElem
		case <-c.writeReq:
			if c.alreadyDisconnect || c.closeCalled {
				c.writeReturn <- false
			} else {
				c.writeReturn <- true
			}
		case <-c.connIDReq:
			//ConnID function called
			if c.connID >= 0 {
				c.connIDAnswer <- c.connID
			}
		}
	}
}

//read messages from server and send the acks directly
func (c *client) readRoutine() {
	for {
		select {
		case <-c.readRountineQuit:
			return
		default:
			var b [MaxMessageByteLen]byte
			realLen, err := c.connection.Read(b[:])
			if err == nil {
				var newMsg Message
				errMarshal := json.Unmarshal(b[:realLen], &newMsg)
				msg := &newMsg
				if errMarshal == nil {
					switch msg.Type {
					case MsgData:
						if checkCorrect(msg) {
							ack := NewAck(msg.ConnID, msg.SeqNum)
							c.writeMsg(ack)
							c.incomeData <- msg
						}
					case MsgAck:
						if msg.SeqNum == 0 {
							c.connMain <- msg.ConnID
						}
						c.newAckSeqNum <- msg.SeqNum
					}
					c.serverAlive <- true
				}
			}
		}
	}
}
