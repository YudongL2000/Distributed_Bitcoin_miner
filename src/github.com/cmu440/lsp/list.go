package lsp

import (
	//"bytes"
	"encoding/json"
	"errors"
	//"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

type node struct {
	prev *node
	next *node
	seqNum int
	msg *Message
}

type list struct {
	head *node
	tail *node
}

func newList() *list {
	newL:= &list{
		head: nil,
		tail: nil,
	}
	return newL
}

func listCheck (L *list) bool {
	if L.head == nil && L.tail != nil {
		return false
	} else if L.head != nil && L.tail == nil{
		return false
	} else if L.head == nil {
		return true
	}
	tracer := L.head
	for tracer != nil {
		if tracer.seqNum != tracer.msg.SeqNum {
			return false
		} 
		if tracer.prev != nil {
			if tracer.prev.next != tracer {
				return false
			}
		}
		if tracer.next != nil {
			if tracer.next.prev != tracer {
				return false
			}
		}
		tracer = tracer.next
	}
	return true
}

func empty(L *list) bool{
	if L.head == nil && L.tail == nil {
		return true
	}
	return false
}

func checkPresent(msg *Message, L *list) bool{
	if empty(L) {
		return false
	} else {
		tracer := L.head
		for tracer != nil {
			if tracer.seqNum == msg.SeqNum {
				return true
			}
			tracer=tracer.next
		}
		return false
	}
}

func listInsert(msg *Message, L *list) {
	newNode = &node{
		prev: nil,
		next: nil,
		seqNum: msg.SeqNum
		msg: msg
	}
	if empty(L) {
		L.head = newNode
		L.tail = newNode
		return
	} else if (L.head == L.tail) && (L.head != nil) {
		first = L.head
		if first.seqNum > msg.SeqNum {
			newNode.next = first
			newNode.prev = nil
			first.prev = newNode
			L.head = newNode
		} else if first.seqNum < msg.SeqNum {
			newNode.prev = first
			newNode.next = nil
			first.next = newNode
			L.tail = newNode
		}
	} else {
		// insert into tail if it's the biggest
		if L.tail.seqNum < msg.SeqNum {
			currentTail := L.tail
			currentTail.next = newNode
			newNode.prev = currentTail
			newNode.next = nil
			L.tail = newNode
			return
		} else if (L.tail.seqNum == msg.SeqNum) {
			return
		}
		//more than one node in list
		tracer := L.head
		for tracer != nil {
			if tracer.seqNum == msg.SeqNum {
				return
			} else if tracer.seqNum > msg.SeqNum {
				if tracer == L.head {
					tracer.prev = newNode
					newNode.next = tracer
					newNode.prev= nil
					L.head = newNode
					return
				} else {
					newPrev := tracer.prev
					newPrev.next = newNode
					newNode.prev = newPrev
					newNode.next = tracer
					tracer.prev = newNode
					return
				}
			}
			tracer = tracer.next
		}
	}
}


func removeSeqNum (num int, L *list) *Message{
	tracer := L.head
	for tracer != nil {
		if tracer.seqNum == num {
			if (tracer.prev == nil) && (tracer.next == nil) {
				//only one element in the list
				L.head = nil
				L.tail = nil
				return tracer.msg
			} else if (tracer.prev == nil) {
				// first one to be removed
				newFirst := tracer.next
				newFirst.prev = nil
				L.head = newFirst
				tracer.next = nil
				return tracer.msg
			} else if (tracer.next == nil) {
				newLast := tracer.prev
				newLast.next = nil
				L.tail = newLast
				tracer.prev = nil
				return tracer.msg
			} else {
				//in the middle
				prevNode := tracer.prev
				nextNode := tracer.next
				prevNode.next = nextNode
				nextNode.prev = prevNode
				return tracer.msg
			}
		}
	}
	return nil
}