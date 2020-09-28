// Contains the implementation of a LSP client.

package lsp

import (
	//"bytes"
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	//"time"
)

const MaxMessgeByteLen = 2000

type client struct {
	params           *Params
	connID           int
	connection       *lspnet.UDPConn
	connectionAck    chan bool
	seqNum           int
	wantedMsg        int
	readResponse     chan *Message
	windowStart      int
	unAcked          []*Message
	unsentBuffer     []*Message
	receivedMsg      []*Message
	incomeAck        chan *Message
	outgoPayload     chan []byte
	recieveData      chan *Message
	readQuit         chan bool
	writeQuit        chan bool
	readRoutineQuit  chan bool
	dataRecieverQuit bool
	quit             chan bool
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
		params:           params,
		connID:           -1,
		connection:       conn,
		connectionAck:    make(chan bool),
		seqNum:           1,
		wantedMsg:        1,
		readResponse:     make(chan *Message),
		windowStart:      -1,
		unAcked:          nil,
		unsentBuffer:     nil,
		receivedMsg:      nil,
		incomeAck:        make(chan *Message),
		outgoPayload:     make(chan []byte),
		recieveData:      make(chan *Message),
		readQuit:         make(chan bool),
		writeQuit:        make(chan bool),
		readRoutineQuit:  make(chan bool),
		dataRecieverQuit: false,
		quit:             make(chan bool),
	}

	go c.readRoutine()
	go c.mainRoutine()
	connectedMsg := NewConnect()
	c.writeMsg(connectedMsg)

	<-c.connectionAck
	return c, nil
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {
	case <-c.readQuit:
		return nil, errors.New("Client disconnected")
	default:
		msg := <-c.readResponse
		c.wantedMsg += 1
		return msg.Payload, nil
	}
}

//unimplemented
func (c *client) message2CheckSum(ID int, seqNum int, size int, payload []byte) uint16 {
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
	select {
	case <-c.writeQuit:
		return errors.New("Connection has been dropped")
	default:
		c.outgoPayload <- payload
		return nil
	}
}

func (c *client) Close() error {
	c.quit <- true
	return nil
}

func (c *client) terminateConnection() {
	c.readRoutineQuit <- true
	c.connection.Close()
}

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

//wait for implementation
func checkCorrupted(m *Message) bool {
	if len(m.Payload) != m.Size {
		return false
	}
	/*
		if message2CheckSum(m.ConnID, m.SeqNum, m.Size, m.Payload) != m.Checksum {
			return false
		}
	*/
	return true
}

func (c *client) sendAllAvailable() {
	counter := 0
	for _, m := range c.unsentBuffer {
		if c.windowStart < 0 {
			c.writeMsg(m)
			c.unAcked = append(c.unAcked, m)
			c.windowStart = m.SeqNum
			counter += 1
		} else if (m.SeqNum <= c.params.WindowSize+c.windowStart-1) && (len(c.unAcked) < c.params.MaxUnackedMessages) {
			c.writeMsg(m)
			c.unAcked = append(c.unAcked, m)
			counter += 1
		} else {
			break
		}
	}
	c.unsentBuffer = c.unsentBuffer[counter:]
}

//maintain the order of the recieved messages
func (c *client) insertMsg(msg *Message) {
	for i, m := range c.receivedMsg {
		if m.SeqNum > msg.SeqNum {
			c.receivedMsg = append(append(c.receivedMsg[:i], msg), c.receivedMsg[i:]...)
			return
		}
	}
	c.receivedMsg = append(c.receivedMsg, msg)
}

func (c *client) mainRoutine() {
	for {
		select {
		case <-c.quit:
			c.readQuit <- true
			c.writeQuit <- true
			c.dataRecieverQuit = true
			if (len(c.unAcked) == 0) && (len(c.unsentBuffer) == 0) {
				c.terminateConnection()
				return
			}
		case ack := <-c.incomeAck:
			if c.dataRecieverQuit && (len(c.unAcked) == 0) && (len(c.unsentBuffer) == 0) {
				c.terminateConnection()
				return
			}
			sn := ack.SeqNum
			if sn < c.windowStart || c.windowStart < 0 {
				if sn == 0 {
					c.connectionAck <- true
				}
			} else {
				for i, storeMsg := range c.unAcked {
					if storeMsg.SeqNum == sn {
						c.unAcked = append(c.unAcked[:i], c.unAcked[i+1:]...)
						if len(c.unAcked) > 0 {
							c.windowStart = c.unAcked[0].SeqNum
						} else {
							//none unAcknoledged message exists
							c.windowStart = -1
						}
						//send the waiting messages in the buffer
						c.sendAllAvailable()
					}
				}
			}
		case msg := <-c.recieveData:
			if c.dataRecieverQuit {
				continue
			} else if checkCorrupted(msg) {
				c.insertMsg(msg)
				if (c.receivedMsg[0]).SeqNum == c.wantedMsg {
					m := c.receivedMsg[0]
					c.receivedMsg = c.receivedMsg[1:]
					c.readResponse <- m
				}
				ackMsg := NewAck(msg.ConnID, msg.SeqNum)
				c.writeMsg(ackMsg)
			}
		case payload := <-c.outgoPayload:
			payloadSize := len(payload)
			checkSum := c.message2CheckSum(c.connID, c.seqNum, payloadSize, payload)
			m := NewData(c.connID, c.seqNum, payloadSize, payload, checkSum)
			c.seqNum += 1
			if c.windowStart < 0 {
				c.unAcked = append(c.unAcked, m)
				c.windowStart = m.SeqNum
				c.writeMsg(m)
			} else if (m.SeqNum <= c.params.WindowSize+c.windowStart-1) && (len(c.unAcked) < c.params.MaxUnackedMessages) {
				c.unAcked = append(c.unAcked, m)
				c.writeMsg(m)
			} else {
				c.unsentBuffer = append(c.unsentBuffer, m)
			}
		}
	}
}

func (c *client) readRoutine() {
	for {
		select {
		case <-c.readRoutineQuit:
			return
		default:
			b := make([]byte, MaxMessgeByteLen)
			realLen, err := c.connection.Read(b)
			if err == nil {
				var newMsg Message
				var msg *Message
				err := json.Unmarshal(b[:realLen], &newMsg)
				msg = &newMsg
				if err == nil {
					switch msg.Type {
					case MsgData:
						c.recieveData <- msg
					case MsgAck:
						c.incomeAck <- msg
					}
				}
			}
		}
	}
}
