package hz

import (
	"errors"
)

type ClientConnectionManager struct {

}

func (manager *ClientConnectionManager) GetOrConnect(address Address, hzUser string, hzPassword string) *Promise {
	connection := NewClientConnection(address)

	promise := connection.Connect(address)
	promise2 := promise.Then(func(obj interface{}) (interface{}, error) {
		connection := obj.(*ClientConnection)
		return connection,nil
	}, func(err error) error{
		return err
	})

	promise3 := promise2.ThenPromise(func(obj interface{}) *Promise {
		connection := obj.(*ClientConnection)
		return authenticate(connection, hzUser, hzPassword)
	}, func(err error) error{
		return err
	})

//	promise4 := promise3.ThenSuccessReturnPromise(func(obj interface{}) *Promise {
//		isAuthenticated := obj.(*bool)
//		if(isAuthenticated){
//			fmt.Println("Hello")
//		}
//		return nil
//	}, func(err error) {
//		fmt.Println("Not Hello")
//	})

	return promise3
}

func authenticate(connection *ClientConnection,hzUser string, hzPassword string) *Promise {
	result := new(Promise)

	result.SuccessChannel = make(chan interface{}, 1)
	result.FailureChannel = make(chan error, 1)

	//////////////////////////////////////////
	request := EncodeRequest(hzUser, hzPassword, nil, nil, true, "GOLANG", 1) //config
	request.SetCorrelationId(1)
	request.SetPartitionId(-1)
	request.SetFlags(BEGIN_END_FLAG)

	connection.socket.Write(request.Buffer)

	rBuffer := make([]byte, 1024)
	readBytes, _ := connection.socket.Read(rBuffer)
	response := CreateForDecode(rBuffer[:readBytes])
	///////////////////////////////////////////////

	go func() {
		authResponse := DecodeResponse(response)
		if authResponse.Status == 0 {
			connection.Address.Host = authResponse.Address.Host
			connection.Address.Port = authResponse.Address.Port
			result.SuccessChannel <- connection
		} else {
			result.FailureChannel <- errors.New("Connection is not authenticated" + connection.Address.String())
		}
	}()

	return result
}