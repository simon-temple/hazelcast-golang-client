package hz

import (
	"encoding/binary"
)

func EncodeQueuePutRequest(name string, byteArray [] byte) *ClientMessage {

	message := CreateForEncode(CalculateSizeStr(&name) + (len(byteArray) + INT_SIZE_IN_BYTES))
	message.SetMessageType(CLIENT_QUEUE_PUT)
	message.AppendStr(&name)
	message.AppendByteArray(byteArray)

	message.UpdateFrameLength()

	return message
}

func EncodeQueuePollRequest(name string, timeout uint64) *ClientMessage {

	message := CreateForEncode(CalculateSizeStr(&name) + INT64_SIZE_IN_BYTES)
	message.SetMessageType(CLIENT_QUEUE_POLL)
	message.AppendStr(&name)
	message.AppendInt64(timeout)

	message.UpdateFrameLength()

	return message
}

func EncodeQueueClearRequest(name string) *ClientMessage {

	message := CreateForEncode(CalculateSizeStr(&name))
	message.SetMessageType(CLIENT_QUEUE_CLEAR)
	message.AppendStr(&name)

	message.UpdateFrameLength()

	return message
}

func calcHash(connection *ClientConnection, key [] byte) int32 {

	// To determine the partition ID of an operation, compute the Murmur Hash (version 3, 32-bit, see https://en.wikipedia.org/wiki/MurmurHash and http s://code.google.com/p/smhasher/wiki/MurmurHash3)
	// of a certain byte-array (which is identified for each message description section) and take the modulus of the result over the total number of partitions. The seed for the Murmur Hash SHOULD
	// be 0x01000193. Most operations with a key parameter use the key parameter byte-array as the data for the hash calculation.

	av := Hash32(key, 0x01000193)
	if av == -2147483648 {
		av = 0
	} else {
		if av < 0 {
			av = -av
		}
	}
	hash := int32(av % connection.partitionCount)

	connection.Logger.Trace("### Hash Calc: murmur3: %d, partition count: %d, hash: %d", av, connection.partitionCount, hash)
	return hash
}

func SendQueueClearRequest(connection *ClientConnection, name string) {

	request := EncodeQueueClearRequest(name)

	request.SetCorrelationId(connection.NextCorrelationId())
	nlbuffer := make([]byte, INT_SIZE_IN_BYTES)
	binary.BigEndian.PutUint32(nlbuffer, uint32(len(name)))

	request.SetPartitionId(calcHash(connection, []byte(append(nlbuffer, name...))))
	request.SetFlags(BEGIN_END_FLAG)

	response, _ := connection.Exchange(request)

	if response.GetMessageType() != 0x0064 {
		connection.Logger.Error("Unexpected response to queue CLEAR request ! Type: 0x%04x", response.GetMessageType())
		if response.GetMessageType() == 0x006d {
			connection.Logger.Error("    Error Code: %d", response.readInt())
			connection.Logger.Error("    Class Name: %s", *response.readString())
		}

	}
}

func SendQueuePollRequest(connection *ClientConnection, name string, timeout uint64) [] byte {

	request := EncodeQueuePollRequest(name, timeout)

	request.SetCorrelationId(connection.NextCorrelationId())
	nlbuffer := make([]byte, INT_SIZE_IN_BYTES)
	binary.BigEndian.PutUint32(nlbuffer, uint32(len(name)))

	request.SetPartitionId(calcHash(connection, []byte(append(nlbuffer, name...))))
	request.SetFlags(BEGIN_END_FLAG)

	response, _ := connection.Exchange(request)

	if response.GetMessageType() != 0x0069 {
		connection.Logger.Error("Unexpected response to queue POLL request ! Type: 0x%04x", response.GetMessageType())
		if response.GetMessageType() == 0x006d {
			connection.Logger.Error("    Error Code: %d", response.readInt())
			connection.Logger.Error("    Class Name: %s", *response.readString())
		}

	} else {

		if !response.readBool() {

			response.readInt()   // length
			response.readBEInt() //partition hash?

			serializerTypeId := response.readBEInt()
			if serializerTypeId != 10 {
				connection.Logger.Error("Queue POLL response, invalid serializer type: %d", serializerTypeId)

			} else {
				byteArray := response.readBEByteArray()
				connection.Logger.Trace("Queue POLL successful to %s, %d bytes received", name, len(byteArray))
				return byteArray
			}
		}
	}

	return nil
}

func SendQueuePutRequest(connection *ClientConnection, name string, byteArray [] byte) {

	connection.Logger.Trace("Send message to queue: %s content: %s", name, string(byteArray))

	// Partition Hash?  I'll set to zero!!
	pbuffer := make([]byte, INT_SIZE_IN_BYTES)
	binary.BigEndian.PutUint32(pbuffer, 0)

	// Serializer TypeID=10
	tbuffer := make([]byte, INT_SIZE_IN_BYTES)
	binary.BigEndian.PutUint32(tbuffer, 10)

	// Length of ByteArray for serializer
	lbuffer := make([]byte, INT_SIZE_IN_BYTES)
	binary.BigEndian.PutUint32(lbuffer, uint32(len(byteArray)))

	b1 := append(lbuffer, byteArray...)
	b2 := append(tbuffer, b1...)
	b3 := append(pbuffer, b2...)

	request := EncodeQueuePutRequest(name, b3)

	request.SetCorrelationId(connection.NextCorrelationId())

	nlbuffer := make([]byte, INT_SIZE_IN_BYTES)
	binary.BigEndian.PutUint32(nlbuffer, uint32(len(name)))

	request.SetPartitionId(calcHash(connection, []byte(append(nlbuffer, name...))))
	request.SetFlags(BEGIN_END_FLAG)

	response, _ := connection.Exchange(request)

	if response.GetMessageType() != 0x0064 {
		connection.Logger.Error("Unexpected response to queue PUT request ! Type: 0x%04x", response.GetMessageType())
		if response.GetMessageType() == 0x006d {
			connection.Logger.Error("    Error Code: %d", response.readInt())
			connection.Logger.Error("    Class Name: %s", *response.readString())
		}

	} else {
		connection.Logger.Trace("Queue PUT successful to %s, %d bytes", name, len(byteArray))
	}
}

func StartQueueListener(connection *ClientConnection, name string) ResponseCallback {

	request := EncodeAddListenerRequest(name)

	request.SetCorrelationId(connection.NextCorrelationId())
	request.SetPartitionId(-1)
	request.SetFlags(BEGIN_END_FLAG)

	response, _ := connection.Exchange(request)

	if response.GetMessageType() != 0x0068 {
		connection.Logger.Error("Unexpected response to queue add listener request ! Type: 0x%04x", response.GetMessageType())
		if response.GetMessageType() == 0x006d {
			connection.Logger.Error("    Error Code: %d", response.readInt())
			connection.Logger.Error("    Class Name: %s", *response.readString())
		}

	} else {
		connection.Logger.Trace("Queue ADD LISTENER successful to %s, registrationId: %s", name, *response.readString())
	}

	cb := connection.Register(request.GetCorrelationId())
	cb.autoRemove = false

	return *cb
}

func EncodeAddListenerRequest(name string) *ClientMessage {

	message := CreateForEncode(CalculateSizeStr(&name) + BYTE_SIZE_IN_BYTES + BYTE_SIZE_IN_BYTES)

	message.SetMessageType(CLIENT_QUEUE_ADD_LISTENER)
	message.AppendStr(&name)
	message.AppendBool(false)
	message.AppendBool(false)

	message.UpdateFrameLength()

	return message
}

func ProcessQueueEvent(clientMessage *ClientMessage, connection *ClientConnection, name string) []byte {

	if !clientMessage.readBool() {
		// Ignore content as we'll poll() for it
	}
	uuid := clientMessage.readString()
	eventType := clientMessage.readInt()

	connection.Logger.Trace("Proccessing queue event: %s, %d", *uuid, eventType)

	if eventType == 1 {
		// An item has been added, so go get it
		return SendQueuePollRequest(connection, name, 0)
	}
	return nil
}
