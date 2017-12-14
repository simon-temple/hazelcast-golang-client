/*
Client Message is the carrier framed data as defined below.
Any request parameter, response or event data will be carried in the payload.

0                   1                   2                   3
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|R|                      Frame Length                           |
+-------------+---------------+---------------------------------+
|  Version    |B|E|  Flags    |               Type              |
+-------------+---------------+---------------------------------+
|                                                               |
+                       CorrelationId                           +
|                                                               |
+---------------------------------------------------------------+
|                        PartitionId                            |
+-----------------------------+---------------------------------+
|        Data Offset          |                                 |
+-----------------------------+                                 |
|                      Message Payload Data                    ...
|
 */

package hz

import (
	"encoding/binary"
	"unicode/utf8"
)

const (
	VERSION = 0
	BEGIN_FLAG = 0x80
	END_FLAG = 0x40
	BEGIN_END_FLAG = BEGIN_FLAG | END_FLAG
	LISTENER_FLAG = 0x01

	PAYLOAD_OFFSET = 18
	SIZE_OFFSET = 0

	FRAME_LENGTH_FIELD_OFFSET = 0
	VERSION_FIELD_OFFSET = FRAME_LENGTH_FIELD_OFFSET + INT_SIZE_IN_BYTES
	FLAGS_FIELD_OFFSET = VERSION_FIELD_OFFSET + BYTE_SIZE_IN_BYTES
	TYPE_FIELD_OFFSET = FLAGS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES
	CORRELATION_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + SHORT_SIZE_IN_BYTES
	PARTITION_ID_FIELD_OFFSET = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES
	DATA_OFFSET_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES
	HEADER_SIZE = DATA_OFFSET_FIELD_OFFSET + SHORT_SIZE_IN_BYTES
)

type ClientMessage struct {
	Buffer      []byte
	writeIndex  int
	readIndex   int
	isRetryable bool
}

/*
	Client Message Constructors
 */

func CreateForDecode(buffer []byte) *ClientMessage {
	msg := new(ClientMessage)
	msg.Buffer = buffer
	msg.isRetryable = false

	return msg
}

func CreateForEncode(payloadSize int) *ClientMessage {
	msg := new(ClientMessage)
	buffer := make([]byte, HEADER_SIZE + payloadSize)
	msg.Buffer = buffer
	msg.SetDataOffset(uint16(HEADER_SIZE))
	msg.writeIndex = HEADER_SIZE
	msg.SetFrameLength(int32(HEADER_SIZE)) //?
	msg.SetPartitionId( -1 )
	msg.isRetryable = false

	return msg
}
/*
	HEADER ACCESSORS
 */

func (msg *ClientMessage) GetFrameLength() int32 {
	return int32(binary.LittleEndian.Uint32(msg.Buffer[FRAME_LENGTH_FIELD_OFFSET:VERSION_FIELD_OFFSET]))
}

func (msg *ClientMessage) SetFrameLength(v int32) {
	binary.LittleEndian.PutUint32(msg.Buffer[FRAME_LENGTH_FIELD_OFFSET:VERSION_FIELD_OFFSET], uint32(v))
}

func (msg *ClientMessage) SetVersion(v uint8) {
	msg.Buffer[VERSION_FIELD_OFFSET] = byte(v)
}

func (msg *ClientMessage) GetFlags() uint8 {
	return msg.Buffer[FLAGS_FIELD_OFFSET]
}

func (msg *ClientMessage) SetFlags(v uint8) {
	msg.Buffer[FLAGS_FIELD_OFFSET] = byte(v)
}

func (msg *ClientMessage) HasFlags(flags uint8) uint8 {
	return msg.GetFlags() & flags
}

func (msg *ClientMessage) GetMessageType() uint16 {
	return binary.LittleEndian.Uint16(msg.Buffer[TYPE_FIELD_OFFSET:CORRELATION_ID_FIELD_OFFSET])
}

func (msg *ClientMessage) SetMessageType(v uint16) {
	binary.LittleEndian.PutUint16(msg.Buffer[TYPE_FIELD_OFFSET:CORRELATION_ID_FIELD_OFFSET], v)
}

func (msg *ClientMessage) GetCorrelationId() int64 {
	return int64(binary.LittleEndian.Uint64(msg.Buffer[CORRELATION_ID_FIELD_OFFSET:PARTITION_ID_FIELD_OFFSET]))
}

func (msg *ClientMessage) SetCorrelationId(val uint64) {
	binary.LittleEndian.PutUint64(msg.Buffer[CORRELATION_ID_FIELD_OFFSET:PARTITION_ID_FIELD_OFFSET], uint64(val))
}

func (msg *ClientMessage) GetPartitionId() int32 {
	return int32(binary.LittleEndian.Uint32(msg.Buffer[PARTITION_ID_FIELD_OFFSET:DATA_OFFSET_FIELD_OFFSET]))
}

func (msg *ClientMessage) SetPartitionId(val int32) {
	binary.LittleEndian.PutUint32(msg.Buffer[PARTITION_ID_FIELD_OFFSET:DATA_OFFSET_FIELD_OFFSET], uint32(val))
}

func (msg *ClientMessage) GetDataOffset() uint16 {
	return binary.LittleEndian.Uint16(msg.Buffer[DATA_OFFSET_FIELD_OFFSET:HEADER_SIZE])
}

func (msg *ClientMessage) SetDataOffset(v uint16) {
	binary.LittleEndian.PutUint16(msg.Buffer[DATA_OFFSET_FIELD_OFFSET:HEADER_SIZE], v)
}

func (msg *ClientMessage) writeOffset() int {
	return int(msg.GetDataOffset()) + msg.writeIndex
}

func (msg *ClientMessage) readOffset() int {
	return int(msg.GetDataOffset()) + msg.readIndex
}

/*
	PAYLOAD
 */

func (msg *ClientMessage) AppendByte(v uint8) {
	msg.Buffer[msg.writeIndex] = byte(v)
	msg.writeIndex += BYTE_SIZE_IN_BYTES
}

func (msg *ClientMessage) AppendInt(v int) {
	binary.LittleEndian.PutUint32(msg.Buffer[msg.writeIndex : msg.writeIndex + INT_SIZE_IN_BYTES], uint32(v))
	msg.writeIndex += INT_SIZE_IN_BYTES
}

func (msg *ClientMessage) AppendInt64(v uint64) {
	binary.LittleEndian.PutUint64(msg.Buffer[msg.writeIndex : msg.writeIndex + INT64_SIZE_IN_BYTES], v)
	msg.writeIndex += INT64_SIZE_IN_BYTES
}

func (msg *ClientMessage) AppendByteArray(arr []byte) {
	length := len(arr)
	//length
	msg.AppendInt(length)
	//copy content
	copy(msg.Buffer[msg.writeIndex : msg.writeIndex + length], arr)
	msg.writeIndex += length
}

func (msg *ClientMessage) AppendStr(str *string) {
	if utf8.ValidString(*str) {
		msg.AppendByteArray([]byte(*str))
	}else {
		//todo dynamic byte array? (look at the below comment)
		buff := make([]byte, 0, len(*str) * 3)
		n := 0
		for _, b := range *str {
			n += utf8.EncodeRune(buff[n:], rune(b))
		}
		//append fixed size slice
		msg.AppendByteArray(buff[0:n])
	}
}

func (msg *ClientMessage) AppendBool(v bool) {
	if v {
		msg.AppendByte(1)
	}else {
		msg.AppendByte(0)
	}
}

/*
	PAYLOAD READ
 */

func (msg *ClientMessage) readBEInt() int32 {
	offset:= msg.readOffset()
	int := int32(binary.BigEndian.Uint32(msg.Buffer[offset:offset + INT_SIZE_IN_BYTES]))
	msg.readIndex += INT_SIZE_IN_BYTES
	return int
}

func (msg *ClientMessage) readInt() int32 {
	offset:= msg.readOffset()
	int := int32(binary.LittleEndian.Uint32(msg.Buffer[offset:offset + INT_SIZE_IN_BYTES]))
	msg.readIndex += INT_SIZE_IN_BYTES
	return int
}

func (msg *ClientMessage) readByte() uint8 {
	byte := byte(msg.Buffer[msg.readOffset()])
	msg.readIndex += BYTE_SIZE_IN_BYTES
	return byte
}

func (msg *ClientMessage) readBool() bool {
	if msg.readByte() == 1 {
		return true
	}else {
		return false
	}
}
func (msg *ClientMessage) readString() *string {
	str := string(msg.readByteArray())
	return &str
}

func (msg *ClientMessage) readBEByteArray() []byte {
	length := msg.readBEInt()
	result := msg.Buffer[msg.readOffset(): msg.readOffset() + int(length)]
	msg.readIndex += int(length)
	return result
}

func (msg *ClientMessage) readByteArray() []byte {
	length := msg.readInt()
	result := msg.Buffer[msg.readOffset(): msg.readOffset() + int(length)]
	msg.readIndex += int(length)
	return result
}

/*
	Helpers
 */

func (msg *ClientMessage) IsRetryable() bool {
	return msg.isRetryable
}

func (msg *ClientMessage) SetIsRetryable(v bool) {
	msg.isRetryable = v
}

func (msg *ClientMessage) UpdateFrameLength() {
	msg.SetFrameLength(int32(msg.writeIndex))
}

/*
	Free methods
 */

func CalculateSizeStr(str *string) int {
	return len(*str) + INT_SIZE_IN_BYTES
}

//func CalculateSizeData()

//func CalculateSizeAddress()
