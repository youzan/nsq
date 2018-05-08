package svc

import "os/signal"

// Create variable signal.Notify function so we can mock it in tests
var signalNotify = signal.Notify

// Service interface contains Start and Stop methods which are called
// when the service is started and stopped. The Init method is called
// before the service is started, and after it's determined if the program
// is running as a Windows Service.
//
// The Start method must be non-blocking.
//
// Implement this interface and pass it to the Run function to start your program.
type Service interface {
	// Init is called before the program/service is started and after it's
	// determined if the program is running as a Windows Service.
	Init(Environment) error

	// Start is called after Init. This method must be non-blocking.
	Start() error

	// Stop is called in response to os.Interrupt, os.Kill, or when a
	// Windows Service is stopped.
	Stop() error
}

// Environment contains information about the environment
// your application is running in.
type Environment interface {
	// IsWindowsService returns true if the program is running as a Windows Service.
	IsWindowsService() bool
}
