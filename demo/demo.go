package main

import (
	".."
	"time"
)

var (
	con *hz.ClientConnection
	logger *StdoutLogger
	pingStopper chan bool
)

func startPinger() {

	pingTicker := time.NewTicker(10 * time.Second)

	go func() {

		for {
			select {
			case <-pingStopper:
				logger.Warn("Bus pinger notified to stop")
				con.Close()
			case <-pingTicker.C:
				hz.SendPing(con)
			}
		}
	}()
}

func main() {

	var cm hz.ClientConnectionManager

	logger = NewLogger()
	pingStopper = make(chan bool)

	logger.Info("demo start")

	hzAdd := hz.Address{
		Host: "localhost",
		Port: 5900,
	}

	p := cm.GetOrConnect( hzAdd, "dev", "dev-pass")

	select {
	case err := <-p.FailureChannel:
		logger.Fatal("Failed to connect to Hazelcast: %v", err)
	case connection := <-p.SuccessChannel:
		logger.Info("Connection Established: %v", connection.(*hz.ClientConnection).Address)
		con = connection.(*hz.ClientConnection)
		con.Logger = logger
		con.InitReadLoop()
	}

	// Keep connection alive
	startPinger()
	// Get node partition info
	hz.SendPartitions(con)
	// Get a proxy to a queue
	hz.SendProxyRequest(con, "myqueue", "hz:impl:queueService")
	// Send a message to the queue
	hz.SendQueuePutRequest(con, "myqueue", []byte("Hello World"))
	// Take a message from the queue or timeout
	ba := hz.SendQueuePollRequest(con, "myqueue", 10000)
	logger.Info("Received message: %s", string(ba))
	// Remove our proxy
	hz.SendProxyDestroyRequest(con, "myqueue", "hz:impl:queueService")

	// Ping a while
	time.Sleep( time.Second * 20)

	pingStopper <- true

	logger.Info("demo end")
}