---
mkskill:
  pos: 10
  in: readme
---

# quevu-forward

A proxy between queues for [quevu](https://github.com/pablo-botella/quevu):
it attends a queue here and delivers it to a hub somewhere else. For
instance, the log of programming errors of an application, forwarded to a
server where the programmers can use it.

> **Component level only.** The two ends are built and tested. The road
> between them (how packets are protected, how the receiver is sure of who
> is sending) is part of the transport and **has not been decided**. What
> is here for HTTP is the plainest thing that works, not that decision.

Where the application runs:

```go
fwd, _ := qforward.Open(qforward.Config{
	Log:       "errors",
	Spool:     "errors.queue", // its queue, on disk
	Transport: qforward.HTTP("https://example.com/logs/in", "shop-1", nil),
})
errors, _ := qkernel.NewQHub("errors", qkernel.WithWriter(fwd))
```

On the server, in whatever web server the application already has:

```go
rcv, _ := qforward.NewReceiver(qforward.ReceiverConfig{
	Into:  func(from, log string) (qkernel.Intake, error) { return inletFor(from, log), nil },
	State: "receiver.state",
})
mux.Handle("/logs/in", rcv) // whatever comes in through this URL
```
