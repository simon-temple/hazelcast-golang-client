package hz

import "fmt"

/*
	Hazelcast Core Objects
 */

type Address struct {

	Host string
	Port int
}

func (address Address) String() string {

	return fmt.Sprintf("Address: host=%s, port=%d", address.Host, address.Port)
}

type Promise struct {

	SuccessChannel chan interface{}
	FailureChannel chan error
}

func (this *Promise) ThenFirst(success func(interface{}) error, failure func(error)) *Promise {

	result := new(Promise)

	result.SuccessChannel = make(chan interface{}, 1)
	result.FailureChannel = make(chan error, 1)

	go func() {
		select {
		case obj := <-this.SuccessChannel:
			newErr := success(obj)
			if newErr == nil {
				result.SuccessChannel <- obj
			} else {
				result.FailureChannel <- newErr
			}
		case err := <-this.FailureChannel:
			failure(err)
			result.FailureChannel <- err
		}
	}()

	return result
}

func (this *Promise) Then(success func(interface{}) (interface{}, error), failure func(error) error) *Promise {

	result := new(Promise)

	result.SuccessChannel = make(chan interface{}, 1)
	result.FailureChannel = make(chan error, 1)

	go func() {
		select {
		case obj := <-this.SuccessChannel:
			newObj, newErr := success(obj)
			if newErr == nil {
				result.SuccessChannel <- newObj
			} else {
				result.FailureChannel <- newErr
			}
		case err := <-this.FailureChannel:
			newErr := failure(err)
			if newErr != nil {
				result.FailureChannel <- newErr
			}
		}
	}()

	return result
}

func (this *Promise) ThenPromise(success func(interface{}) *Promise, failure func(error) error) *Promise {

	result := new(Promise)

	result.SuccessChannel = make(chan interface{}, 1)
	result.FailureChannel = make(chan error, 1)

	go func() {
		select {
		case obj := <-this.SuccessChannel:
			newPromise := success(obj)
				select {
				case newObj := <-newPromise.SuccessChannel:
					result.SuccessChannel <- newObj
				case newErr := <-newPromise.FailureChannel:
					result.FailureChannel <- newErr
				}
		case err := <-this.FailureChannel:
			newErr := failure(err)
			if newErr != nil {
				result.FailureChannel <- newErr
			}
		}
	}()

	return result
}
