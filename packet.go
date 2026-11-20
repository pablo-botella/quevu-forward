// Package qforward moves a log from one place to another: a proxy between
// queues. It attends a queue here and delivers it to a hub somewhere
// else, for instance the log of programming errors of an application to
// a server where the programmers can use it.
//
// It is two components, one at each end ("forwarder" and "receiver" are
// working names):
//
//   - Forwarder is a persistent consumer of a hub. It guarantees its own
//     queue: what could not be sent yet is kept on disk, so it survives
//     the other end being down and the application being restarted. It
//     sends in small packets.
//   - Receiver takes those packets at the other end and feeds them into
//     the flow there: a hub and its consumers.
//
// What carries a packet from one to the other is a Transport, and it is
// not here. This package is the two components and nothing of the road
// between them: how packets travel, how they are protected and how the
// receiver knows who sent them belong to the transport, which is decided
// and built separately.
//
// Sending is "at least once": a packet is sent again until the receiver
// confirms it, so the same packet may arrive twice. The receiver copes
// with that by the position each chunk had at its origin.
package qforward

import (
	"context"

	quevu "github.com/pablo-botella/quevu-kernel"
)

// Packet is a small stretch of one log on its way to the other end:
// consecutive chunks, with the positions and times they had at the
// origin.
type Packet struct {
	// Log is the name of the log the chunks belong to.
	Log string
	// Chunks are in ascending order of position.
	Chunks []quevu.Chunk
}

// Last returns the position of the last chunk of the packet, or zero.
func (p Packet) Last() uint64 {
	if len(p.Chunks) == 0 {
		return 0
	}
	return p.Chunks[len(p.Chunks)-1].Seq
}

// Transport carries a packet to the other end and brings back the
// confirmation. It is the road between a Forwarder and a Receiver, and
// everything about the road is its business: the protocol, the
// protection of what travels, and making sure the receiver knows who the
// sender is.
//
// Send returns the position up to which the receiver confirms the log,
// which means those chunks are in a hub at the other end. An error, or a
// confirmation short of the packet's last position, means the packet has
// to be sent again.
type Transport interface {
	Send(ctx context.Context, p Packet) (confirmed uint64, err error)
}

// TransportFunc makes a Transport of a function.
type TransportFunc func(ctx context.Context, p Packet) (uint64, error)

func (f TransportFunc) Send(ctx context.Context, p Packet) (uint64, error) { return f(ctx, p) }
