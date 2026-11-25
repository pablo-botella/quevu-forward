---
mkskill:
  pos: 5
  in: ai*
---

# quevu-forward - agent notes

Go package `github.com/pablo-botella/quevu-forward`, imported as
`qforward`. Two components and the interface between them:

- `Forwarder`: a `qkernel.Writer` and `qkernel.SeqWriter` with a queue on
  disk (kept with `quevu/qtxt`).
- `Receiver`: `Accept(from, Packet)`, and an `http.Handler`.
- `Transport`: what carries a `Packet` and brings back the confirmation.
  `HTTP(...)` is a plain one; `TransportFunc` adapts a function.

Scope decided by Pablo: component level now, the transport later. Do not
add encryption, signatures, keys or certificates here until that is
discussed; a proposal exists (HPKE to the receiver's public key, Ed25519
per forwarder) and is noted in QUEVU-NOTES.md, not approved. The receiver
owns no server, no mux and no routes: those are the hosting application's.
