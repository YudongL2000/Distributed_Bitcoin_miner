// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"github.com/cmu440/lspnet"
	"fmt"
	"encoding/json"
	"bytes"
	"log"
)

type server struct {
	udpAddr *lspnet.UDPAddr,
	clientList *clientList
}

type client struct {
	connID int
	next *client
}

type clientList struct {
	clients *client
	clientNum int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", port)
	s := &server{
		udpAddr: 	addr,
		clientList: newClientList()
	}

	go s.Start()

	return s, err
}

// Start the server, listen for any incoming connections
func (s *server) Start() {
	for {
		conn, err := lspnet.ListenUDP("udp", s.udpAddr)
		if err {
			break
		}

		go s.MainRoutine()
		go s.ReadRoutine()

	}
}

func (s *server) MainRoutine() {
	for {
		select {
		default:
			break
		}
	}
}

func (s *server) ReadRoutine() {
	for {
		select {
		default:
			break
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return -1, nil, errors.New("not yet implemented")
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
func newClientList() {
	cl = &clientList {
		clients: nil,
		clientNum: 0
	}

	return cl
}