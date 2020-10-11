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
	acked       bool
	seqNum      int
	backOff     int
	epochPassed int
	msg         *Message
	//sentSuccess chan bool
}

type client struct {
	params        *Params
	connID        int
	connection    *lspnet.UDPConn
	connectionAck chan bool
	seqNum        int
	wantedMsg     int
	ackConn       int
	readResponse  chan *Message
	window        []*sentQueueElem
	windowStart   int
	windowUnAcked int
	unsentBuffer  []*sentQueueElem
	//receivedMsg       []*Message
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
	//ackToSend       chan *Message
	connIDReq    chan bool
	connIDAnswer chan int
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
		params:        params,
		connID:        -1,
		connection:    conn,
		connectionAck: make(chan bool),
		seqNum:        1,
		wantedMsg:     1,
		ackConn:       0,
		readResponse:  make(chan *Message),
		window:        nil,
		windowStart:   0,
		windowUnAcked: 0,
		unsentBuffer:  nil,
		//receivedMsg:       nil,
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
		//ackToSend:         make(chan *Message),
		connIDReq:    make(chan bool),
		connIDAnswer: make(chan int),
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
	// TODO: remove this line when you are ready to begin implementing this method.
	msg := <-c.readResponse
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

//================================================= End of api Functions ====================================

func (c *client) SendConnAck() {
	msg := NewAck(c.connID, 0)
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

//================================================= Start of window functions ================================

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
			c.unsentBuffer = c.unsentBuffer[remain:]
		}
	}
}

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

func (c *client) ProcessAck(sn int) {
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
			c.SendAllAvailable()
			//fmt.Printf("currently there're %v number of elements in window\n", len(c.window))
		}
	}
}

func (c *client) AddNewWindowElem(newElem *sentQueueElem) {
	if len(c.window) == 0 {
		c.window = append(c.window, newElem)
		c.windowStart = newElem.seqNum
		c.windowUnAcked = 1
		c.WriteMsg(newElem.msg)
		//go c.WindowElemRountine(newElem)
	} else if (newElem.seqNum-c.window[0].seqNum < c.params.WindowSize) && (c.windowUnAcked < c.params.MaxUnackedMessages) {
		c.window = append(c.window, newElem)
		c.windowUnAcked += 1
		c.WriteMsg(newElem.msg)
		//go c.WindowElemRountine(newElem)
	} else {
		c.unsentBuffer = append(c.unsentBuffer, newElem)
	}
}

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
/*
func (c *client)insertionIdx(sn int) int {
	for j,m := range c.receivedMsg {
		if m.SeqNum >= sn {
			return j
		}
	}
	return -1
}

func (c *client) printList () {
	for j,m := range(c.receivedMsg) {
		if j<len(c.receivedMsg)-1 {
			fmt.Printf("%v ", m.SeqNum)
		} else {
			fmt.Printf("%v\n",m.SeqNum)
		}
	}
}

func (c *client) StoreData(msg *Message) {
	fmt.Printf("before insertion: ")
	c.printList()
	if len(c.receivedMsg) == 0 {
		c.receivedMsg = append(c.receivedMsg, msg)
		fmt.Printf("after insertion: ")
		c.printList()
		return
	} else if (c.receivedMsg[len(c.receivedMsg)-1].SeqNum < msg.SeqNum) {
		c.receivedMsg = append(c.receivedMsg, msg)
		fmt.Printf("after insertion: ")
		c.printList()
		return
	}
	idx := c.insertionIdx(msg.SeqNum)
	fmt.Printf("index: %v\n", idx)
	if c.receivedMsg[idx].SeqNum == msg.SeqNum {
		fmt.Printf("after insertion: ")
		c.printList()
		return
	}
	if idx >= 0 {
		c.receivedMsg = append(c.receivedMsg[idx+1:], c.receivedMsg[:idx]...)
		c.receivedMsg[idx] = msg
	} else {
		c.receivedMsg = append(c.receivedMsg, msg)
	}
	fmt.Printf("after insertion: ")
	c.printList()
	return
}
*/
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
	connStable := false
	aliveConfirm := false
	connConfirm := false
	disconnected := false
	wantQuit := false
	connectionMsg := NewConnect()
	c.WriteMsg(connectionMsg)
	//=========================================== Need to set the connection message caller =====================================
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
				c.SendConnAck()
			} else {
				aliveConfirm = false
			}
		case <-dropConnTimer.C:
			if connConfirm == false {
				c.connectionAck <- false
				disconnected = true
				c.disconnect <- true
				continue
			}
			if connStable == false {
				c.disconnect <- true
				disconnected = true
				if wantQuit {
					c.quitConfirm <- true
				}
			} else {
				dropConnTimer = time.NewTicker(time.Duration(c.params.EpochLimit*c.params.EpochMillis) * time.Millisecond)
				connStable = false
			}
		case <-c.timerDrop:
			return
		case <-c.receiveSth:
			connStable = true
			//heartBeatTimer = time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
			//dropConnTimer = time.NewTicker(time.Duration(c.params.EpochLimit*c.params.EpochMillis) * time.Millisecond)
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
			c.ProcessAck(sn)
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
		/*
			if len(c.receivedMsg) > 0 {
				if c.receivedMsg[0].SeqNum == c.wantedMsg {
					fmt.Printf("wanted %v\n", c.wantedMsg)
					fmt.Printf("first msg %v\n", c.receivedMsg[0].SeqNum)
					readRes := c.receivedMsg[0]
					c.receivedMsg = c.receivedMsg[1:]
					c.readResponse <- readRes
					c.wantedMsg += 1
				} else if c.alreadyDisconnect {
					c.readResponse <- nil
				}
			} else if c.alreadyDisconnect {
				c.readResponse <- nil
			}*/
		//printList(c.recieveList)
		if (len(c.readResponse) == 0) {
			if (!empty(c.recieveList)) && (c.recieveList.head.seqNum == c.wantedMsg) {
				readRes := sliceHead(c.recieveList)
				c.readResponse <- readRes
				//fmt.Printf("info matched %v\n", c.wantedMsg)
				//printList(c.recieveList)
				c.wantedMsg +=1 
			} else if c.alreadyDisconnect {
				c.readResponse <- nil
			}
		}
		select {
		case <-c.mainQuit:
			c.quitReqTime <- true
		case <-c.quitConfirm:
			c.alreadyDisconnect = true
			c.AbortAll()
			return
		case <-c.disconnect:
			if c.alreadyDisconnect == false {
				c.alreadyDisconnect = true
			}
		case ID := <-c.connMain:
			if c.ackConn == 0 {
				c.connID = ID
				c.ackConn = 1
			}
		case msg := <-c.incomeData:
			if msg.SeqNum < c.wantedMsg {
				continue
			} else {
				c.StoreData(msg)
				//printList(c.recieveList)
			}
		case payload := <-c.outgoPayload:
			//fmt.Printf("have payload to send")
			payloadSize := len(payload)
			checkSum := c.Message2CheckSum(c.connID, c.seqNum, payloadSize, payload)
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
			if c.alreadyDisconnect {
				c.writeReturn <- false
			} else {
				c.writeReturn <- true
			}
		case <-c.connIDReq:
			if c.connID >= 0 {
				c.connIDAnswer <- c.connID
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
