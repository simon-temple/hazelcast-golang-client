package hz

func encodePartitionRequest() *ClientMessage {

	message := CreateForEncode(0)
	message.SetMessageType(CLIENT_GETPARTITIONS)
	message.UpdateFrameLength()

	return message
}

func SendPartitions(connection *ClientConnection) {

	request := encodePartitionRequest()

	request.SetCorrelationId(connection.NextCorrelationId())
	request.SetFlags(BEGIN_END_FLAG)

	response, _ := connection.Exchange(request)

	if response.GetMessageType() != 0x006c {
		connection.Logger.Error("Unexpected response to ping! Type: 0x%04x", response.GetMessageType())
	} else {
		elements := response.readInt()
		connection.Logger.Trace("Member Partitions: %d", elements)

		for i := int32(0); i < elements; i++ {

			addressHost := response.readString()
			addressPort := response.readInt()
			partitionId := response.readInt()

			connection.Logger.Trace("Member: %d, Partition: %s,%d,%d", i, *addressHost, addressPort, partitionId)

			if connection.Address.Host == *addressHost && int32(connection.Address.Port) == addressPort {
				connection.Logger.Trace("Connected host has partition id: %d", partitionId)
				connection.partitionCount = partitionId
			}
		}

	}
}
