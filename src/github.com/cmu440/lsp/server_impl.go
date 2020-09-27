// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"github.com/cmu440/lspnet"
	"fmt"
	"encoding/json"
	"bytes"
	"log"
	"strconv"
)

type server struct {
	udpAddr *lspnet.UDPAddr
	clientList *clientList
	readMsg chan []byte
}

// Stores all information of a client, defined when a connection is 
// established
type clientInfo struct {
	connID int
	conn *lspnet.UDPConn
	next *clientInfo
}

// Stores a linked list of all clients, defined upon server creation
type clientList struct {
	first *clientInfo
	last *clientInfo
	counter int // counter for creating connID
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", ":" + strconv.Itoa(port))
	s := &server{
		udpAddr: 	addr,
		clientList: newClientList(),
	}

	go s.Start()

	return s, err
}

// Start the server, listen for any incoming connections
func (s *server) Start() {
	for {
		conn, err := lspnet.ListenUDP("udp", s.udpAddr)
		if err != nil {
			break
		}

		c := &clientInfo{
			connID: s.clientList.counter + 1,
			conn: conn,
			next: nil,
		}
		s.addClient(c)

		go s.MainRoutine(c)
		go s.ReadRoutine(c)
		go s.WriteRoutine(c)

	}
}

func (s *server) MainRoutine(c *clientInfo) {
	for {
		select {
		default:
			break
		}
	}
}

func (s *server) ReadRoutine(c *clientInfo) {
	for {
		select {
		default:
			buff := make([]byte, 2000)
			len, cliAddr, err := c.conn.ReadFromUDP(buff[0:])

			var msg Message
			if json.Unmarshal(buff[0:len], &msg) != nil {
				continue
			}
		}
	}
}

func (s *server) WriteRoutine(c *clientInfo) {
	for {
		select {
		default:
			break
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	for {
		select {
		default:
		}
	}
}

func (s *server) Write(connId int, payload []byte) error {
	return errors.New("not yet implemented")
}

func (s *server) CloseConn(connId int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}


// ============================= Helper Functions =============================
func newClientList() *clientList {
	cl := &clientList {
		first: nil,
		last: nil,
		counter: 0,
	}

	return cl
}

func (s *server) addClient(c *clientInfo) {
	if s.clientList.last == nil {
		s.clientList.first = c
		s.clientList.last = c
	} else {
		s.clientList.last.next = c
		s.clientList.last = c
	}
	s.clientList.counter++
}