// Contains the implementation of a LSP client.

package lsp

import (
	//"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

const MaxMessgeByteLen = 2000

type sentQueueElem struct {
	acked       bool
	seqNum      int
	backOff     int
	epochPassed int
	msg         *Message
	//sentSuccess chan bool
}

type client struct {
	params            *Params
	connID            int
	connection        *lspnet.UDPConn
	connectionAck     chan bool
	seqNum            int
	wantedMsg         int
	ackConn           int
	readResponse      chan *Message
	readEmpty         chan bool
	window            []*sentQueueElem
	windowStart       int
	windowUnAcked     int
	unsentBuffer      []*sentQueueElem
	recieveList       *list
	connMain          chan int
	outgoPayload      chan []byte
	newWindowElem     chan *sentQueueElem
	newAckSeqNum      chan int
	writeReq          chan bool
	writeReturn       chan bool
	incomeData        chan *Message
	alreadyDisconnect bool
	readRountineQuit  chan bool
	mainQuit          chan bool
	quitReqTime       chan bool
	quitConfirm       chan bool
	disconnect        chan bool
	timerDrop         chan bool
	closeFinish       chan bool
	receiveSth        chan bool
	connIDReq         chan bool
	connIDAnswer      chan int
	closeCalled       bool
	timeConnIDReq     chan bool
	timeAnswerID      chan int
}

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
		receiveSth:        make(chan bool),
		connIDReq:         make(chan bool),
		connIDAnswer:      make(chan int),
		closeCalled:       false,
		timeConnIDReq:     make(chan bool),
		timeAnswerID:      make(chan int),
	}

	go c.ReadRoutine()
	go c.MainRoutine()
	go c.TimeRoutine()
	connectedInfo := <-c.connectionAck
	if connectedInfo == false {
		c.Close()
		return nil, errors.New("connection failed")
	}
	//fmt.Printf("connection acknowledged\n")
	return c, nil
}

func (c *client) ConnID() int {
	c.connIDReq <- true
	answer := <-c.connIDAnswer
	return answer
}

func (c *client) Read() ([]byte, error) {
	//log("-------client called read, waiting........")
	msg := <-c.readResponse
	c.readEmpty <- true
	//fmt.Printf("read received message\n")
	if msg == nil {
		fmt.Printf("got error message\n")
		return nil, errors.New("read request denied, connection failed")
	}
	//log("-------client called read " + msg.String())
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
	fmt.Printf("client %v ready to close\n", c.connID)
	c.mainQuit <- true
	<-c.closeFinish
	return nil
}

func (c *client) SendConnAck(id int) {
	msg := NewAck(id, 0)
	c.WriteMsg(msg)
}

func minHelper(a int, b int) int {
	if a >= b {
		return b
	} else {
		return a
	}
}

func (c *client) WriteMsg(msg *Message) error {
	//log("client write " + msg.String())
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

func message2CheckSum(ID int, seqNum int, size int, payload []byte) uint16 {
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

func checkCorrect(m *Message) bool {
	actualLen := len(m.Payload)
	wantedLen := m.Size
	usefulPayload := m.Payload
	if actualLen >= wantedLen {
		usefulPayload = m.Payload[:wantedLen]
		m.Payload = usefulPayload
	}
	if message2CheckSum(m.ConnID, m.SeqNum, m.Size, usefulPayload) != m.Checksum {
		return false
	}
	return true
}

//================================================= Start of window functions ================================

//if there's extra space in the window, we move as many window elements as
//we can from the unsent buffer
func (c *client) SendAllAvailable() bool{
	if len(c.unsentBuffer) == 0 {
		return false
	}
	firstUnsent := c.unsentBuffer[0].seqNum
	if len(c.window) == 0 {
		numSent := minHelper(c.params.MaxUnackedMessages, len(c.unsentBuffer))
		c.windowStart = firstUnsent
		c.windowUnAcked = numSent
		c.window = c.unsentBuffer[:numSent]
		for _,elem := range c.window {
			c.WriteMsg(elem.msg)
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
			for _,elem := range newSent {
				c.WriteMsg(elem.msg)
			}
			c.unsentBuffer = c.unsentBuffer[remain:]
			return true
		}
	}
}

//if the first few messages in the window is acked, then we need to shift
//the window backwards
func (c *client) RemoveFinished() {
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
func (c *client) ProcessAck(sn int) bool{
	if (c.windowStart >= 0) && (c.windowStart <= sn) {
		idx := sn - c.windowStart
		if c.window[idx].acked == false {
			//c.window[idx].sentSuccess <- true
			c.window[idx].acked = true
			c.windowUnAcked = c.windowUnAcked - 1
			if idx == 0 {
				//fmt.Printf("removing window element with index %v\n", idx)
				c.RemoveFinished()
			}
			res :=c.SendAllAvailable()
			return res
			//fmt.Printf("currently there're %v number of elements in window\n", len(c.window))
		} else {
			return false
		}
	} else {
		return false
	}
}

// add a new window element to the window
//immediately send once after sending
func (c *client) AddNewWindowElem(newElem *sentQueueElem) {
	if len(c.window) == 0 {
		c.window = append(c.window, newElem)
		c.windowStart = newElem.seqNum
		c.windowUnAcked = 1
		c.WriteMsg(newElem.msg)
	} else if (newElem.seqNum-c.window[0].seqNum < c.params.WindowSize) && (c.windowUnAcked < c.params.MaxUnackedMessages) {
		c.window = append(c.window, newElem)
		c.windowUnAcked += 1
		c.WriteMsg(newElem.msg)
	} else {
		c.unsentBuffer = append(c.unsentBuffer, newElem)
	}
}

// add to epoch counter in all window elements
//resend if backoff is reached
func (c *client) UpdateWindow() bool {
	sendSth := false
	for _, element := range c.window {
		if element.acked == false {
			if element.epochPassed >= element.backOff {
				c.WriteMsg(element.msg)
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

//store the message information in a double linked list
func (c *client) StoreData(msg *Message) {
	listInsert(msg, c.recieveList)
}

func (c *client) AbortAll() {
	c.connection.Close()
	c.timerDrop <- true
	c.readRountineQuit <- true
	c.closeFinish <- true
	return
}

func (c *client) TimeRoutine() {
	heartBeatTimer := time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
	dropConnTimer := time.NewTicker(time.Duration(c.params.EpochLimit*c.params.EpochMillis) * time.Millisecond)
	aliveConfirm := false
	connConfirm := false
	disconnected := false
	wantQuit := false
	connectionMsg := NewConnect()
	c.WriteMsg(connectionMsg)
	for {
		select {
		case <-heartBeatTimer.C:
			heartBeatTimer = time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
			if disconnected {
				continue
			}
			if connConfirm == false {
				c.WriteMsg(connectionMsg)
				continue
			}
			resendMsg := c.UpdateWindow()
			aliveConfirm = resendMsg || aliveConfirm
			if aliveConfirm == false {
				c.timeConnIDReq <- true
				id := <- c.timeAnswerID
				c.SendConnAck(id)
			} else {
				aliveConfirm = false
			}
		case <-dropConnTimer.C:
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
			return
		case <-c.receiveSth:
			dropConnTimer = time.NewTicker(time.Duration(c.params.EpochLimit*c.params.EpochMillis) * time.Millisecond)
		case newElem := <-c.newWindowElem:
			c.AddNewWindowElem(newElem)
			//fmt.Printf("sending new message %v\n", newElem.seqNum)
			aliveConfirm = true
		case sn := <-c.newAckSeqNum:
			if sn == 0 {
				if connConfirm == false {
					connConfirm = true
					c.connectionAck <- true
				}
				continue
				//heart beat or ack
			}
			//fmt.Printf("recieve ack for window msg %v\n", sn)
			sendNew :=c.ProcessAck(sn)
			aliveConfirm = aliveConfirm || sendNew
			//fmt.Printf("finished processing ack for window msg %v\n",sn)
			if wantQuit && (len(c.window) == 0) && (len(c.unsentBuffer) == 0) {
				c.quitConfirm <- true
			}
		case <-c.quitReqTime:
			wantQuit = true
			if disconnected {
				c.quitConfirm <- true
			} else if wantQuit && (len(c.window) == 0) && (len(c.unsentBuffer) == 0) {
				c.quitConfirm <- true
			}
		}
	}
}

func (c *client) MainRoutine() {
	for {
		select {
		case <-c.mainQuit:
			if c.closeCalled == false {
				c.quitReqTime <- true
				c.closeCalled = true
			}
		case <-c.quitConfirm:
			c.alreadyDisconnect = true
			c.AbortAll()
			fmt.Printf("Client %v terminated\n", c.connID)
			return
		case <-c.disconnect:
			c.alreadyDisconnect = true
			addTerminatingElem(c.recieveList)
			//fmt.Printf("server disconnected the client\n")
			if (len(c.readResponse) == 0) && (c.recieveList.head.seqNum == -1) {
				fmt.Printf("pushing error message\n")
				c.readResponse <- nil
			}
		case <-c.readEmpty:
			if c.closeCalled {
				continue
			}
			//fmt.Printf("read channel empty!\n")
			//printList(c.recieveList)
			if len(c.readResponse) == 0 {
				if (!empty(c.recieveList)) && (c.recieveList.head.seqNum == c.wantedMsg) {
					readRes := sliceHead(c.recieveList)
					c.readResponse <- readRes
					c.wantedMsg += 1
				} else if (!empty(c.recieveList)) && (c.recieveList.head.seqNum == -1) {
					fmt.Printf("repushing error message\n")
					c.readResponse <- nil
				}
			}
		case ID := <-c.connMain:
			if c.ackConn == 0 {
				c.connID = ID
				c.ackConn = 1
			}
		case msg := <-c.incomeData:
			if c.closeCalled {
				continue
			}
			if msg.SeqNum >= c.wantedMsg {
				c.StoreData(msg)
				//printList(c.recieveList)
				if len(c.readResponse) == 0 {
					if (!empty(c.recieveList)) && (c.recieveList.head.seqNum == c.wantedMsg) {
						readRes := sliceHead(c.recieveList)
						c.readResponse <- readRes
						//fmt.Printf("info matched %v\n", c.wantedMsg)
						//printList(c.recieveList)
						c.wantedMsg += 1
					} else if (!empty(c.recieveList)) && (c.recieveList.head.seqNum == -1) {
						c.readResponse <- nil
					}
				}
			}
		case payload := <-c.outgoPayload:
			if c.closeCalled {
				continue
			}
			//fmt.Printf("have payload to send")
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
			if c.connID >= 0 {
				c.connIDAnswer <- c.connID
			}
		case <- c.timeConnIDReq:
			c.timeAnswerID <-c.connID
		}
	}
}

//read messages from server and send the acks directly
func (c *client) ReadRoutine() {
	for {
		select {
		case <-c.readRountineQuit:
			return
		default:
			var b [MaxMessgeByteLen]byte
			realLen, err := c.connection.Read(b[:])
			if err == nil {
				//fmt.Printf("\nreceived server\n")
				var newMsg Message
				errMarshal := json.Unmarshal(b[:realLen], &newMsg)
				msg := &newMsg
				//log("client read " + msg.String())
				if errMarshal == nil {
					switch msg.Type {
					case MsgData:
						if checkCorrect(msg) {
							ack := NewAck(msg.ConnID, msg.SeqNum)
							c.WriteMsg(ack)
							//c.ackToSend <- ack
							c.incomeData <- msg
						}
						//fmt.Printf("incoming data received %v\n", msg.SeqNum)
					case MsgAck:
						//fmt.Printf("incoming acknowledge for request %v\n", msg.SeqNum)
						if msg.SeqNum == 0 {
							c.connMain <- msg.ConnID
						}
						c.newAckSeqNum <- msg.SeqNum
					}
					c.receiveSth <- true
				}
			}
		}
	}
}
