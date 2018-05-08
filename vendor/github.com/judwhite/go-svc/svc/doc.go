/*
Package svc helps you write Windows Service executables without getting in the way of other target platforms.

To get started, implement the Init, Start, and Stop methods to do
any work needed during these steps.

Init and Start cannot block. Launch long-running your code in a new Goroutine.

Stop may block for a short amount of time to attempt clean shutdown.

Call svc.Run() with a reference to your svc.Service implementation to start your program.

When running in console mode Ctrl+C is treated like a Stop Service signal.

For a full guide visit https://github.com/judwhite/go-svc
*/
package svc
