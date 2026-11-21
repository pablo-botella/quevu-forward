package qforward

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/pablo-botella/quevu"
)

// ReceiverConfig sets up a Receiver.
type ReceiverConfig struct {
	// Into says which hub the log of a sender goes to. from is who sent
	// the packet, as the transport established it, and log the name the
	// sender gave the log. Returning an error refuses the packet: the
	// sender keeps it and tries again later.
	//
	// It is what keeps logs apart: two senders, or two logs of one
	// sender, are two different answers.
	Into func(from, log string) (quevu.Intake, error)
	// State is the path of the file where the receiver remembers how far
	// it has taken each log, so that a packet that arrives again after a
	// restart is recognized. Empty: it remembers only while it runs.
	State string
	// Sender says who a request comes from, when the receiver is mounted
	// on a web server. It is the hosting application's to answer, since
	// it is the one that knows how the request was authenticated; an
	// error refuses the request. Nil: the name the packet gives itself
	// is taken at its word, which is only as trustworthy as the road.
	Sender func(*http.Request) (string, error)
}

// Receiver takes the packets of one or more forwarders and feeds them
// into hubs.
//
// A packet may arrive more than once: the sender repeats it until it
// hears the confirmation, and a confirmation can be lost on the way
// back. The receiver remembers, for each log of each sender, the last
// position it handed to a hub, and skips what it has already taken.
type Receiver struct {
	cfg ReceiverConfig

	mu    sync.Mutex
	taken map[string]uint64
	dirty bool
}

// NewReceiver returns a receiver, with what a previous run remembered.
func NewReceiver(cfg ReceiverConfig) (*Receiver, error) {
	if cfg.Into == nil {
		return nil, errors.New("qforward: no way to tell where a log goes")
	}
	r := &Receiver{cfg: cfg, taken: make(map[string]uint64)}
	if cfg.State != "" {
		b, err := os.ReadFile(cfg.State)
		switch {
		case err == nil:
			if err := json.Unmarshal(b, &r.taken); err != nil {
				return nil, fmt.Errorf("qforward: %s is damaged: %w", cfg.State, err)
			}
		case !errors.Is(err, os.ErrNotExist):
			return nil, err
		}
	}
	return r, nil
}

// key names one log of one sender. The separator cannot be typed into
// either name by accident.
func key(from, log string) string { return from + "\x00" + log }

// Accept takes a packet that the transport received from the sender it
// names, and returns the position up to which that log is now in a hub.
// The transport sends that number back as the confirmation.
//
// A chunk is confirmed only once a hub has accepted it. If the hub
// refuses one, what came before it in the packet is confirmed and the
// rest is not: the sender will bring it again.
func (r *Receiver) Accept(from string, p Packet) (confirmed uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key(from, p.Log)
	confirmed = r.taken[k]
	if len(p.Chunks) == 0 {
		return confirmed, nil
	}
	into, err := r.cfg.Into(from, p.Log)
	if err != nil {
		return confirmed, fmt.Errorf("qforward: no place for log %q of %q: %w", p.Log, from, err)
	}
	if into == nil {
		return confirmed, fmt.Errorf("qforward: no place for log %q of %q", p.Log, from)
	}
	timed, _ := into.(quevu.TimedIntake)
	for _, c := range p.Chunks {
		if c.Seq <= confirmed {
			// Seen before: this packet, or part of it, came twice.
			continue
		}
		// The chunk keeps the time it had at its origin when the hub can
		// be told; its position is the next one of the hub it joins.
		if timed != nil {
			err = timed.AppendAt(c.Data, c.Time)
		} else {
			err = into.Append(c.Data)
		}
		if err != nil {
			err = fmt.Errorf("qforward: the hub did not take position %d of log %q of %q: %w", c.Seq, p.Log, from, err)
			break
		}
		confirmed = c.Seq
	}
	if confirmed != r.taken[k] {
		r.taken[k] = confirmed
		r.dirty = true
		// Remembered before it is confirmed. If writing it down fails,
		// the chunks are in the hub all the same, so they are confirmed
		// anyway: the worst that follows a crash is that they arrive
		// twice. It is tried again with the next packet, and on Close.
		r.save()
	}
	return confirmed, err
}

// save writes what the receiver remembers, aside and then into place.
func (r *Receiver) save() error {
	if r.cfg.State == "" || !r.dirty {
		return nil
	}
	b, err := json.Marshal(r.taken)
	if err != nil {
		return err
	}
	tmp := r.cfg.State + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, r.cfg.State); err != nil {
		return err
	}
	r.dirty = false
	return nil
}

// Taken returns the position up to which a log of a sender has been
// handed to a hub.
func (r *Receiver) Taken(from, log string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.taken[key(from, log)]
}

// Close writes what the receiver remembers, if a previous attempt to
// write it failed.
func (r *Receiver) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.save()
}
