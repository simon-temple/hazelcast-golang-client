package hz

const (
	REQUEST_TYPE = CLIENT_AUTHENTICATION
	RESPONSE_TYPE = 107
	RETRYABLE = true
)

type ResponseParameters struct {
	Status byte
	Address *Address
	Uuid *string
	OwnerUuid *string
	SerializationVersion uint8
}

func CalculateSize(username string, password string, uuid *string, ownerUuid *string, isOwnerConnection bool, clientType string, serializationVersion uint8) int {
	dataSize := 0
	dataSize += CalculateSizeStr(&username)
	dataSize += CalculateSizeStr(&password)
	dataSize += BOOLEAN_SIZE_IN_BYTES
	if uuid != nil {
		dataSize += CalculateSizeStr(uuid)
	}
	dataSize += BOOLEAN_SIZE_IN_BYTES
	if ownerUuid != nil {
		dataSize += CalculateSizeStr(ownerUuid)
	}
	dataSize += BOOLEAN_SIZE_IN_BYTES
	dataSize += CalculateSizeStr(&clientType)
	dataSize += BYTE_SIZE_IN_BYTES
	return dataSize
}

func EncodeRequest(username string, password string, uuid *string, ownerUuid *string, isOwnerConnection bool, clientType string, serializationVersion uint8) *ClientMessage{
	payloadSize := CalculateSize(username, password, uuid, ownerUuid, isOwnerConnection, clientType, serializationVersion)
	message := CreateForEncode(payloadSize)
	message.SetMessageType(REQUEST_TYPE)
	message.SetIsRetryable(RETRYABLE)
	message.AppendStr(&username)
	message.AppendStr(&password)
	message.AppendBool(uuid == nil)
	if uuid != nil{
		message.AppendStr(uuid)
	}
	message.AppendBool(ownerUuid == nil)
	if uuid != nil{
		message.AppendStr(ownerUuid)
	}
	message.AppendBool(isOwnerConnection)
	message.AppendStr(&clientType)
	message.AppendByte(serializationVersion)
	message.UpdateFrameLength()

	return message
}

//todo toObject bool??
func DecodeResponse(message *ClientMessage) *ResponseParameters{

	parameters := new(ResponseParameters)
	parameters.Status = message.readByte()
	parameters.Address = nil
	if !(message.readBool()) {
		parameters.Address = new(Address)
		parameters.Address.Host = *message.readString()
		parameters.Address.Port = int(message.readInt())
	}
	parameters.Uuid = nil
	if !(message.readBool()) {
		uuid := message.readString()
		parameters.Uuid = uuid
	}
	parameters.OwnerUuid = nil
	if !(message.readBool()) {
		parameters.OwnerUuid = message.readString()
	}
	parameters.SerializationVersion = message.readByte()

	return parameters
}


