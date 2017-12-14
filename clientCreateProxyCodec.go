package hz

func EncodeProxyCreateRequest(connection *ClientConnection, name string, serviceName string) *ClientMessage {

	payloadSize := calculatePayloadSize(name, serviceName, connection.Address.Host)
	message := CreateForEncode(payloadSize)
	message.SetMessageType(CLIENT_CREATEPROXY)
	message.AppendStr(&name)
	message.AppendStr(&serviceName)

	message.AppendStr(&connection.Address.Host)
	message.AppendInt(connection.Address.Port)

	message.UpdateFrameLength()

	return message
}

func EncodeProxyDestroyRequest(name string, serviceName string) *ClientMessage {

	payloadSize := calculatePayloadSizeBase(name, serviceName)
	message := CreateForEncode(payloadSize)
	message.SetMessageType(CLIENT_DESTROYPROXY)
	message.AppendStr(&name)
	message.AppendStr(&serviceName)

	message.UpdateFrameLength()

	return message
}

func calculatePayloadSizeBase(name string, serviceName string) int {

	dataSize := 0
	dataSize += CalculateSizeStr(&name)
	dataSize += CalculateSizeStr(&serviceName)
	return dataSize
}

func calculatePayloadSize(name string, serviceName string, host string) int {

	dataSize := calculatePayloadSizeBase(name, serviceName)
	dataSize += CalculateSizeStr(&host)
	dataSize += INT_SIZE_IN_BYTES
	return dataSize
}

func SendProxyDestroyRequest(connection *ClientConnection, name string, serviceName string) {

	request := EncodeProxyDestroyRequest(name, serviceName)

	request.SetCorrelationId(connection.NextCorrelationId())
	request.SetFlags(BEGIN_END_FLAG)

	response, _ := connection.Exchange(request)
	if response.GetMessageType() != 0x0064 {
		connection.Logger.Error("Unexpected response to proxy destroy ! Type: 0x%04x", response.GetMessageType())
	} else {
		connection.Logger.Trace("Proxy destroyed without error: %s on %s", name, serviceName)
	}
}

func SendProxyRequest(connection *ClientConnection, name string, serviceName string) {

	request := EncodeProxyCreateRequest(connection, name, serviceName)

	request.SetCorrelationId(connection.NextCorrelationId())
	request.SetFlags(BEGIN_END_FLAG)

	response, _ := connection.Exchange(request)

	if response.GetMessageType() != 0x0064 {
		connection.Logger.Error("Unexpected response to proxy request ! Type: 0x%04x", response.GetMessageType())
	} else {
		connection.Logger.Trace("Proxy configured for: %s on %s", name, serviceName)
	}
}
