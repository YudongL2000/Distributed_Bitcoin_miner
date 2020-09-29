// Contains the implementation of a LSP client.

package lsp

import (
	//"bytes"
	"encoding/json"
	"errors"
	//"fmt"
	"github.com/cmu440/lspnet"
	//"time"
)

const MaxMessgeByteLen = 2000

type client struct {
	params        *Params
	connID        int
	connection    *lspnet.UDPConn
	connectionAck chan bool
	seqNum        int
	wantedMsg     int
	ackConn       bool
	readResponse  chan *Message
	unAcked       []*Message
	unsentBuffer  []*Message
	receivedMsg   []*Message
	incomeAck     chan *Message
	outgoPayload  chan []byte
	incomeData    chan *Message
	readQuit      chan bool
	writeQuit     chan bool
	quit          chan bool
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
		ackConn:       false,
		readResponse:  make(chan *Message),
		unAcked:       nil,
		unsentBuffer:  nil,
		receivedMsg:   nil,
		incomeAck:     make(chan *Message),
		outgoPayload:  make(chan []byte),
		incomeData:    make(chan *Message),
		readQuit:      make(chan bool),
		writeQuit:     make(chan bool),
		quit:          make(chan bool),
	}

	go c.ReadRoutine()
	go c.MainRoutine()
	connectedMsg := NewConnect()
	c.WriteMsg(connectedMsg)
	<-c.connectionAck
	//fmt.Printf("connection acknowledged\n")
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
		if len(m.Payload) != m.Size {
			return false
		}
		if c.Message2CheckSum(m.ConnID, m.SeqNum, m.Size, m.Payload) != m.Checksum {
			return false
		}
	*/
	return true
}

func (c *client) SendAllAvailable() {
	counter := 0
	for _, m := range c.unsentBuffer {
		if len(c.unAcked) == 0 {
			c.WriteMsg(m)
			c.unAcked = append(c.unAcked, m)
			counter += 1
		} else if (m.SeqNum <= c.params.WindowSize+c.unAcked[0].SeqNum-1) && (len(c.unAcked) < c.params.MaxUnackedMessages) {
			c.WriteMsg(m)
			c.unAcked = append(c.unAcked, m)
			counter += 1
		} else {
			break
		}
	}
	c.unsentBuffer = c.unsentBuffer[counter:]
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
	ack := NewAck(msg.ConnID, msg.SeqNum)
	c.WriteMsg(ack)
	return
}

func (c *client) MainRoutine() {
	for {
		if len(c.receivedMsg) > 0 {
			if c.receivedMsg[0].SeqNum == c.wantedMsg {
				readRes := c.receivedMsg[0]
				c.receivedMsg = c.receivedMsg[1:]
				c.readResponse <- readRes
			}
		}
		select {
		case ack := <-c.incomeAck:
			sn := ack.SeqNum
			c.connID = ack.ConnID
			if len(c.unAcked) == 0 {
				if sn == 0 && c.ackConn == false {
					c.ackConn = true
					c.connectionAck <- true
				}
			} else if c.unAcked[0].SeqNum <= sn {
				idx := -1
				for i, storeMsg := range c.unAcked {
					if storeMsg.SeqNum == sn {
						idx = i
						break
					}
				}
				if idx >= 0 {
					c.unAcked = append(c.unAcked[:idx], c.unAcked[idx+1:]...)
					c.SendAllAvailable()
				}
			}
		case msg := <-c.incomeData:
			if c.CheckCorrect(msg) {
				c.StoreData(msg)
			}
		case payload := <-c.outgoPayload:
			//fmt.Printf("have payload to send")
			payloadSize := len(payload)
			checkSum := c.Message2CheckSum(c.connID, c.seqNum, payloadSize, payload)
			m := NewData(c.connID, c.seqNum, payloadSize, payload, checkSum)
			c.seqNum += 1
			if len(c.unAcked) == 0 {
				c.unAcked = append(c.unAcked, m)
				c.WriteMsg(m)
			} else if (m.SeqNum <= c.params.WindowSize+c.unAcked[0].SeqNum-1) && (len(c.unAcked) < c.params.MaxUnackedMessages) {
				c.unAcked = append(c.unAcked, m)
				c.WriteMsg(m)
			} else {
				c.unsentBuffer = append(c.unsentBuffer, m)
			}
		}

	}
}

func (c *client) ReadRoutine() {
	for {
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
					//fmt.Printf("incoming data received %v\n", msg.SeqNum)
					c.incomeData <- msg
				case MsgAck:
					//fmt.Printf("incoming acknowledge for request %v\n", msg.SeqNum)
					c.incomeAck <- msg
				}
			}
		}
	}
}
