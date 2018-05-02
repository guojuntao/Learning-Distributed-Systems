package raftkv

import (
	"log"
)

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	Dup      = "Dup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID int64
	ReqID   int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID int64
	ReqID   int64 // need ?
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

// debug log
const debugLevel = 0xffff

const getLog = 0x0001
const putAppendLog = 0x0002

const serverLog = 0x0010
const clientLog = 0x0020

const recvApplyMsgLog = 0x0100
const readPersistLog = 0x0200

func DPrintf(level uint, format string, a ...interface{}) (n int, err error) {
	if debugLevel&level != 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintln(level uint, a ...interface{}) (n int, err error) {
	if debugLevel&level != 0 {
		log.Println(a...)
	}
	return
}
