package raft

import "log"

// const debugLevel = installSnapshotLog | startLog | stateLog | appendEntriesLog
// TODO 通过环境变量设置 debugLevel
const debugLevel = 0xfffff

const rpcRecvLog = 0x0001
const candidateSendLog = 0x0002
const leaderSendLog = 0x0004

const rpcSendLog = 0x0010
const startLog = 0x0020
const stateLog = 0x0040

const getRandTimeLog = 0x0100
const persistLog = 0x0200
const applyLog = 0x0400

const requestVoteLog = 0x1000
const appendEntriesLog = 0x2000
const installSnapshotLog = 0x4000

const saveSnapshotLog = 0x10000

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
