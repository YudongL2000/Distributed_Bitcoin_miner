//contains the functions for checksum and truncating messages
//used by both client and server

package lsp

//calculate the value of checksum from different attributes of a message
func message2CheckSum(ID int, seqNum int, size int, payload []byte) uint16 {
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

//check if message is correct by the checksum and truncate the message payload if necessary
func checkCorrect(m *Message) bool {
	actualLen := len(m.Payload)
	wantedLen := m.Size
	usefulPayload := m.Payload
	if actualLen >= wantedLen {
		usefulPayload = m.Payload[:wantedLen]
		m.Payload = usefulPayload
	}
	if message2CheckSum(m.ConnID, m.SeqNum, m.Size, usefulPayload) != m.Checksum {
		return false
	}
	return true
}
