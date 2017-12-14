package hz

func EncodePingRequest() *ClientMessage {

	message := CreateForEncode(0)
	message.SetMessageType(CLIENT_PING)
	message.UpdateFrameLength()

	return message
}

func SendPing(connection *ClientConnection) {

	request := EncodePingRequest()

	request.SetCorrelationId(connection.NextCorrelationId())
	request.SetFlags(BEGIN_END_FLAG)

	response, err := connection.Exchange(request)

	if nil == err {
		if response.GetMessageType() != 0x0064 {
			connection.Logger.Error("Unexpected response to ping! Type: 0x%04x", response.GetMessageType())
		} else {
			connection.Logger.Trace("Ping!")
		}
	}
}
