package hz

import (
	"net"
	"fmt"
	"errors"
	"strconv"
	"encoding/binary"
	"sync"
	"time"
)

// An external logging impl for feedback
type ILogging interface {

	Trace(string, ...interface{})
	Info(string, ...interface{})
	Warn(string, ...interface{})
	Error(string, ...interface{})
	Fatal(string, ...interface{})
}

// Callback data as all responses are async
type ResponseCallback struct {

	NotifyChannel chan *ClientMessage
	autoRemove    bool
}

type ClientConnection struct {

	Address        Address
	readBuffer     []byte
	cid            uint64
	partitionCount int32
	socketMutex    *sync.Mutex
	socket         net.Conn

	responsesMutex *sync.Mutex
	responses      map[int64]*ResponseCallback

	Logger ILogging
	Closed bool

	QueueSerializerId uint32
}

const (

	DEFAULT_EXCHANGE_TIMEOUT_MILLIS = 1000 * 60 * 2 // 2 mins
)

func NewClientConnection(address Address) *ClientConnection {

	connection := new(ClientConnection)
	connection.Address = address
	connection.readBuffer = make([]byte, 0) //todo
	connection.socketMutex = &sync.Mutex{}
	connection.responsesMutex = &sync.Mutex{}
	connection.responses = make(map[int64]*ResponseCallback)
	connection.cid = 1
	connection.QueueSerializerId = 0

	return connection
}

// Ensure a unique id on each message exchange
func (this *ClientConnection) NextCorrelationId() uint64 {

	this.cid += 1

	return this.cid
}

func (this *ClientConnection) Close() {

	this.Logger.Trace("Closing connection: %v", this.socket)
	this.socket.Close()
	this.Closed = true;
}

func (this *ClientConnection) Connect(address Address) *Promise {

	result := new(Promise)

	result.SuccessChannel = make(chan interface{}, 1)
	result.FailureChannel = make(chan error, 1)

	go func() {
		socket, err := net.Dial("tcp", this.Address.Host+":"+strconv.Itoa(this.Address.Port))
		this.socket = socket
		if err == nil {
			this.Closed = false
			this.socket.Write([]byte(CLIENT_BINARY_NEW))
			result.SuccessChannel <- this
		} else {
			result.FailureChannel <- errors.New(fmt.Sprintf("Could not connect to address: %s, err(%v)", this.Address.String(), err))
		}
	}()

	return result
}

// A single socket read loop with message distribution to registered callbacks
func (this *ClientConnection) InitReadLoop() {

	go func() {

		flBuffer := make([]byte, INT_SIZE_IN_BYTES)

		for {
			i, err := this.socket.Read(flBuffer)
			if nil != err {
				this.Logger.Error("Unexpected error reading frame length! %v - read loop aborted!", err)
				return;
			}
			for i < len(flBuffer) {
				j, err2 := this.socket.Read(flBuffer[i:])
				if nil != err2 {
					this.Logger.Error("Unexpected error reading frame length! %v - read loop aborted!", err)
					return;
				}
				i = i + j
			}
			frameLength := binary.LittleEndian.Uint32(flBuffer[0:])
			fBuffer := make([]byte, frameLength-INT_SIZE_IN_BYTES)

			i, err = this.socket.Read(fBuffer)
			if nil != err {
				this.Logger.Error("Unexpected error reading message! %v - read loop aborted!", err)
				return;
			}
			if i < len(fBuffer) {
				j, err2 := this.socket.Read(fBuffer[i:])
				if nil != err2 {
					this.Logger.Error("Unexpected error reading message! %v - read loop aborted!", err)
					return;
				}
				i = i + j
			}

			msg := new(ClientMessage)
			msg.Buffer = append(flBuffer, fBuffer...)

			cid := msg.GetCorrelationId()

			this.responsesMutex.Lock()
			cb, ok := this.responses[cid]
			if ok {
				if cb.autoRemove {
					delete(this.responses, cid)
					this.Logger.Trace("Removed correlation id from responses map: %d", cid)
				}
				this.responsesMutex.Unlock()
				go func() {
					cb.NotifyChannel <- msg
				}()
			} else {
				this.Logger.Error("Failed to find correlation id: %d using response message of type: 0x%04x! Message receiver is now BLOCKED!!", cid, msg.GetMessageType())
				this.responsesMutex.Unlock()
			}
		}
	}()
}

func (this *ClientConnection) Exchange(msg *ClientMessage) (*ClientMessage, error) {

	return this.ExchangeWithTimeout(msg, DEFAULT_EXCHANGE_TIMEOUT_MILLIS)
}

func (this *ClientConnection) ExchangeWithTimeout(msg *ClientMessage, timeout time.Duration) (*ClientMessage, error) {

	this.socketMutex.Lock()

	this.Logger.Trace("====> Sending: cid=%d, type=0x%02x, partitionid=%d, framelength=%d, flags=0x%02x, dataoffset=%d", msg.GetCorrelationId(), msg.GetMessageType(), msg.GetPartitionId(), msg.GetFrameLength(), msg.GetFlags(), msg.GetDataOffset())

	n, err := this.socket.Write(msg.Buffer)
	if nil != err {
		this.socketMutex.Unlock()
		this.Logger.Error("Fatal socket error on write: %v\n", err)
		this.Close()
		return nil, err
	}
	if n != len(msg.Buffer) {
		this.socketMutex.Unlock()
		this.Close()
		return nil, errors.New(fmt.Sprintf("Fatal socket error: Incomplete write to socket! buffer size=%d, written=%d\r\n", len(msg.Buffer), n))
	}

	this.socketMutex.Unlock()

	cb := this.Register(msg.GetCorrelationId())
	cb.autoRemove = true

	select {
	case response := <-cb.NotifyChannel:
		return response, nil
	case <-time.After(time.Millisecond * timeout):
		// call timed out
		return nil, errors.New(fmt.Sprintf("Message exchange timeout. No response received in: %d millis", timeout))
	}
}

// Receive callback registration based on message correlation id
func (this *ClientConnection) Register(correlationId int64) *ResponseCallback {

	responseCallback := ResponseCallback{}
	responseCallback.NotifyChannel = make(chan *ClientMessage)

	this.responsesMutex.Lock()

	this.responses[correlationId] = &responseCallback

	this.responsesMutex.Unlock()

	return &responseCallback
}
