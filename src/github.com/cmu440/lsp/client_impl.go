// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"time"
	"bytes"
	"encoding/json"
	"github.com/cmu440/lspnet"
)

const MaxMessgeByteLen = 2000

type client struct {
	params   *Params
	connID   int
	connection  *lspnet.UDPConn
	seqNum      int
	seqNumChan  chan int
	seqNumReq   chan bool
	sendAvailable chan bool
	windowStart   int
	unAcked       []*Message
	incomeMessage chan *Message
	outgoMessage  chan *Message
	readQuit      chan bool
	writeQuit   chan bool
	quit        chan bool
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
	conn, connError = lspnet.DialUDP("udp", nil, addr)
	if connError != nil {
		return nil, connError
	}
	c := &client{
		params:   params,
		connID:   -1,
		connection:  conn,
		seqNum:      1,
		seqNumChan:  make(chan int),
		seqNumReq:   make(chan bool),
		sendAvailable: make(chan bool),
		windowStart: -1,
		unAcked:     nil,
		incomeMessage:   make(chan *Message),
		outgoMessage:    make(chan *Message),
		readQuit:        make(chan bool),
		writeQuit:   make(chan bool),
		quit:        make(chan bool),
	}

	c.sendAvailable <- true
	go c.readRoutine()
	go c.mainRoutine()
	connectedMsg := NewConnect()
	res , err := json.Marshal(connectedMsg)

	return nil, errors.New("not yet implemented")
}

func (c *client) ConnID() int {
	return c.connID
}




func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {
	case <- c.quit:
		return (nil, errors.New("Client disconnected"))
	default:



	} // Blocks indefinitely.
	return nil, errors.New("not yet implemented")
}



//unimplemented
func (c *client) message2CheckSum(ID int, seqNum int, payload []byte) uint32{
	var checksum uint 
	checksum := 0
	checksum += Int2Checksum(connID)
	checksum += Int2Checksum(seqNum)
	checksum += Int2Checksum(size)
	checksum += ByteArray2Checksum(payload)
	return checksum
}


func (c *client) Write(payload []byte) error {
	select{
	case <-c.writeQuit:
		return errors.New("Connection has been droped")
	default:
		c.sendAvailable <- true
		c.seqNumReq <- true
		newSeqNum :<- c.seqNumChan
		payloadSize := len(payload)
		checkSum := message2CheckSum(c.connID, seqNum, payloadSize, payload)
		c.outgoMessage <- NewData(c.connID, newSeqNum, payloadSize, payload, checkSum)
		return nil
	}
	//return errors.New("not yet implemented")
}

func (c *client) Close() error {
	c.quit <- true
	return nil
}

func (c *client)processAllRemain(){

}

func (c *client)terminateAll(){

}



func (c *client)writeMsg(msg *Message) error {
	content, err = json.Marshal(msg)
	if err != nil {
		return err
	} else {
		res, writeErr = c.connection.Write(content)
		if writeErr != nil {
			return writeErr
		}
		return nil
	}
}

//wait for implementation
func checkCorrupted(m *Message) bool{
	if len(m.Payload) != m.Size {
		return false
	}
	if message2CheckSum(m.ConnID, m.SeqNum, m.Size, m.Payload) != m.Checksum {
		return false
	}
	return true
}

func checkIfFull


func (c *client) mainRoutine() {
	for {
		select{
		case <-c.quit:
			c.processAllRemain()
			c.terminateAll()
		case <- c.seqNumReq:
			c.seqNumChan <- c.seqNum
			c.seqNum += 1 
			if (c.windowStart <0) || (c.windowStart > 0 && (m.SeqNum <= c.params.WindowSize + c.windowStart -1)) {
				<- c.sendAvailable 
			}
		case m :<- c.incomeMessage:
			switch m.Type {
			case MsgData:
				if (checkCorrupted(m)){
					ackMsg := NewAck(m.ConnID, m.SeqNum)
					c.outgoMessage <- ackMsg
				} 
			case MsgAck:
				sn := m.SeqNum
				if sn < c.windowStart || c.windowStart <0 {
					continue
				} else {
					for i, snum := range c.unAcked {
						if snum == sn {
							c.unAcked = append(c.unAcked[:i], c.unAcked[i+1:]...)
							if len(c.unAcked) > 0 {
								c.windowStart = c.unAcked[0]
							} else {
								//none unAcknoledged message exists
								c.windowStart = -1
							}
							break
						}
					}
				}
			}
		case <- c.sendAvailable:
			case m :<- outgoMessage:
				if c.windowStart < 0 {
					c.unAcked = append(c.unAcked, m.SeqNum)
					c.windowStart = m.SeqNum
					writeMsg(m) 
				} else if (m.SeqNum <= c.params.WindowSize + c.windowStart -1) && (len(c.unAcked) < c.params.MaxUnackedMessages) {
					c.unAcked = append(c.unAcked, m.SeqNum)
					writeMsg(m)				
				}
		}
	}
}

func (c *client) readRoutine() {
	for {
		select{
		case <- c.readQuit:
			return 
		default:
			b := make([]byte, MaxMessgeByteLen)
			realLen, err := c.connection.Read(b)
			if err == nil {
				var newMsg Message
				err :=  json.Unmarshal(b[:realLen], &newMsg)
				if err != nil {
					return err
				} else {
					c.incomeMessage <- &newMsg
				}
			}
		}
	}
}