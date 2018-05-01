package raft

import "log"

// const debugLevel = snapshotLog | startLog | stateLog | appendEntriesLog
const debugLevel = 0

const rpcRecvLog = 0x0001
const candidateSendLog = 0x0002
const leaderSendLog = 0x0004

const rpcSendLog = 0x0010
const startLog = 0x0020
const stateLog = 0x0040

const getRandTimeLog = 0x0100
const persistLog = 0x0200

const requestVoteLog = 0x1000
const appendEntriesLog = 0x2000
const snapshotLog = 0x4000

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
