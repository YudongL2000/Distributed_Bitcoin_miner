// Contains the implementation of a LSP client.

package lsp

import (
	//"bytes"
	"encoding/json"
	"errors"
	//"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

const MaxMessgeByteLen = 2000

type sentQueueElem struct {
	seqNum      int
	backOff     int
	epochPassed int
	msg         *Message
	sentSuccess chan bool
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
	window            []*sentQueueElem
	windowStart       int
	windowUnAcked     int
	unsentBuffer      []*sentQueueElem
	receivedMsg       []*Message
	incomeAck         chan *Message
	outgoPayload      chan []byte
	writeReq          chan bool
	writeReturn       chan bool
	incomeData        chan *Message
	alreadyDisconnect bool
	readRountineQuit  chan bool
	mainQuit          chan bool
	wantQuit          bool
	disconnect        chan bool
	timerDrop         chan bool
	closeFinish       chan bool
	receiveSth        chan bool
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
		readResponse:      make(chan *Message),
		window:            nil,
		windowStart:       0,
		windowUnAcked:     0,
		unsentBuffer:      nil,
		receivedMsg:       nil,
		incomeAck:         make(chan *Message),
		outgoPayload:      make(chan []byte),
		writeReq:          make(chan bool),
		writeReturn:       make(chan bool),
		incomeData:        make(chan *Message),
		alreadyDisconnect: false,
		readRountineQuit:  make(chan bool),
		mainQuit:          make(chan bool),
		wantQuit:          false,
		disconnect:        make(chan bool),
		timerDrop:         make(chan bool),
		closeFinish:       make(chan bool),
		receiveSth:        make(chan bool),
	}

	go c.ReadRoutine()
	go c.MainRoutine()
	go c.HeartBeat()
	connectedMsg := NewConnect()
	newElem := &sentQueueElem{
		seqNum:      0,
		backOff:     0,
		epochPassed: 0,
		msg:         connectedMsg,
		sentSuccess: make(chan bool),
	}
	go c.WindowElemRountine(newElem)
	connectedInfo := <-c.connectionAck
	newElem.sentSuccess <- true
	if connectedInfo == false {
		/*time out warning*/
		c.Close()
		return nil, errors.New("connection failed")
	}
	//fmt.Printf("connection acknowledged\n")
	return c, nil
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	msg := <-c.readResponse
	if msg == nil {
		return nil, errors.New("read request denied, connection failed")
	}
	c.wantedMsg += 1
	return msg.Payload, nil
}

//unimplemented
func (c *client) Message2CheckSum(ID int, seqNum int, size int, payload []byte) uint16 {
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

func (c *client) WindowElemRountine(element *sentQueueElem) {
	//fmt.Printf("Start sending iteratively for message: %v\n",element.msg.SeqNum)
	c.WriteMsg(element.msg)
	timer := time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
	for {
		select {
		case <-element.sentSuccess:
			//fmt.Printf("sending Rountine ended for message: %v\n",element.seqNum)
			return
		case <-timer.C:
			if element.epochPassed >= element.backOff {
				timer = time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
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
			} else {
				element.epochPassed += 1
			}
		}
	}
}

func (c *client) SendConnAck() {
	msg := NewAck(c.connID, 0)
	c.WriteMsg(msg)
}

func (c *client) HeartBeat() {
	heartBeatTimer := time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
	dropConnTimer := time.NewTicker(time.Duration(c.params.EpochLimit*c.params.EpochMillis) * time.Millisecond)
	for {
		select {
		case <-heartBeatTimer.C:
			heartBeatTimer = time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
			c.SendConnAck()
		case <-dropConnTimer.C:
			if c.ackConn == 0 {
				c.connectionAck <- false
				c.ackConn = -1
				return
			} else {
				c.disconnect <- true
			}
		case <-c.timerDrop:
			return
		case <-c.receiveSth:
			heartBeatTimer = time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
			dropConnTimer = time.NewTicker(time.Duration(c.params.EpochLimit*c.params.EpochMillis) * time.Millisecond)
		}
	}
}

func minHelper(a int, b int) int {
	if a >= b {
		return b
	} else {
		return a
	}
}

func (c *client) Close() error {
	c.mainQuit <- true
	<-c.closeFinish
	return nil
}

func (c *client) WriteMsg(msg *Message) error {
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

func (c *client) CheckCorrect(m *Message) bool {
	/*
		actualLen := len(m.Payload)
		wantedLen := m.Size
		usefulPayload := m.Payload
		if actualLen >= wantedLen {
			usefulPayload = m.Payload[:wantedLen]
		}
		if c.Message2CheckSum(m.ConnID, m.SeqNum, m.Size, usefulPayload) != m.Checksum {
			return false
		}
	*/
	return true
}

func (c *client) SendAllAvailable() {
	if len(c.unsentBuffer) == 0 {
		return
	}
	firstUnsent := c.unsentBuffer[0].seqNum
	if len(c.window) == 0 {
		numSent := minHelper(c.params.MaxUnackedMessages, len(c.unsentBuffer))
		c.windowStart = firstUnsent
		c.windowUnAcked = numSent
		c.window = c.unsentBuffer[:numSent]
		c.unsentBuffer = c.unsentBuffer[numSent:]
		for _, elem := range c.window {
			go c.WindowElemRountine(elem)
		}
		return
	} else {
		remain := minHelper(c.params.WindowSize-(firstUnsent-c.windowStart), len(c.unsentBuffer))
		remain = minHelper(remain, c.params.MaxUnackedMessages-c.windowUnAcked)
		if remain <= 0 {
			return
		} else {
			newSent := c.unsentBuffer[:remain]
			c.window = append(c.window, newSent...)
			c.windowUnAcked += remain
			for _, elem := range newSent {
				go c.WindowElemRountine(elem)
			}
			c.unsentBuffer = c.unsentBuffer[remain:]
		}
	}
}

func (c *client) RemoveFinished() {
	counter := 0
	for _, elem := range c.window {
		if elem.msg == nil {
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

func (c *client) StoreData(msg *Message) {
	var idx int
	idx = -1
	for j, m := range c.receivedMsg {
		if m.SeqNum == msg.SeqNum {
			return
		} else if m.SeqNum > msg.SeqNum {
			idx = j
			break
		}
	}
	if idx >= 0 {
		c.receivedMsg = append(append(c.receivedMsg[:idx], msg), c.receivedMsg[idx:]...)
	} else {
		c.receivedMsg = append(c.receivedMsg, msg)
	}
	return
}

func (c *client) AbortAll() {
	c.connection.Close()
	c.timerDrop <- true
	c.readRountineQuit <- true
	c.closeFinish <- true
	return
}

func (c *client) MainRoutine() {
	for {
		if len(c.receivedMsg) > 0 {
			if c.receivedMsg[0].SeqNum == c.wantedMsg {
				readRes := c.receivedMsg[0]
				c.receivedMsg = c.receivedMsg[1:]
				c.readResponse <- readRes
			} else if c.alreadyDisconnect {
				c.readResponse <- nil
			}
		} else if c.alreadyDisconnect {
			c.readResponse <- nil
		}

		select {
		case <-c.mainQuit:
			c.wantQuit = true
			if c.alreadyDisconnect {
				c.AbortAll()
				return
			} else if (len(c.window) == 0) && (len(c.unsentBuffer) == 0) {
				c.AbortAll()
				return
			}
		case <-c.disconnect:
			if c.alreadyDisconnect == false {
				/*cancel all sending rountine*/
				for _, elem := range c.window {
					if elem.msg != nil {
						elem.sentSuccess <- true
					}
				}
				c.alreadyDisconnect = true
				if c.wantQuit {
					c.AbortAll()
					return
				}
			}
		case ack := <-c.incomeAck:
			sn := ack.SeqNum
			if len(c.window) == 0 {
				/*recieve heart beat and initial connection ack*/
				if sn == 0 && c.ackConn == 0 {
					c.ackConn = 1
					c.connID = ack.ConnID
					c.connectionAck <- true
				}
			} else if c.windowStart <= sn {
				idx := sn - c.windowStart
				if c.window[idx].msg != nil {
					c.window[idx].sentSuccess <- true
					c.window[idx].msg = nil
					c.windowUnAcked = c.windowUnAcked - 1
					if idx == 0 {
						//fmt.Printf("removing window element with index %v\n", idx)
						c.RemoveFinished()
					}
					c.SendAllAvailable()
					//fmt.Printf("currently there're %v number of elements in window\n", len(c.window))
				}
			}
			if c.wantQuit && (len(c.window) == 0) && (len(c.unsentBuffer) == 0) {
				c.AbortAll()
				c.alreadyDisconnect = true
				return
			}
		case msg := <-c.incomeData:
			ack := NewAck(msg.ConnID, msg.SeqNum)
			c.WriteMsg(ack)
			if msg.SeqNum < c.wantedMsg {
				continue
			} else {
				c.StoreData(msg)
			}
		case payload := <-c.outgoPayload:
			//fmt.Printf("have payload to send")
			payloadSize := len(payload)
			checkSum := c.Message2CheckSum(c.connID, c.seqNum, payloadSize, payload)
			m := NewData(c.connID, c.seqNum, payloadSize, payload, checkSum)
			c.seqNum += 1
			newElem := &sentQueueElem{
				seqNum:      m.SeqNum,
				backOff:     0,
				epochPassed: 0,
				msg:         m,
				sentSuccess: make(chan bool),
			}
			if len(c.window) == 0 {
				c.window = append(c.window, newElem)
				c.windowStart = m.SeqNum
				c.windowUnAcked = 1
				go c.WindowElemRountine(newElem)
			} else if (m.SeqNum-c.window[0].seqNum < c.params.WindowSize) && (c.windowUnAcked < c.params.MaxUnackedMessages) {
				c.window = append(c.window, newElem)
				c.windowUnAcked += 1
				go c.WindowElemRountine(newElem)
			} else {
				c.unsentBuffer = append(c.unsentBuffer, newElem)
			}
		case <-c.writeReq:
			if c.alreadyDisconnect {
				c.writeReturn <- false
			} else {
				c.writeReturn <- true
			}
		}
	}
}

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
				if errMarshal == nil {
					switch msg.Type {
					case MsgData:
						if c.CheckCorrect(msg) {
							c.incomeData <- msg
						}
						//fmt.Printf("incoming data received %v\n", msg.SeqNum)
					case MsgAck:
						//fmt.Printf("incoming acknowledge for request %v\n", msg.SeqNum)
						c.incomeAck <- msg
					}
					c.receiveSth <- true
				}
			}
		}
	}
}
