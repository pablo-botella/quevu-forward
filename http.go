package qforward

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pablo-botella/quevu/qkernel"
)

// The receiver does not own a web server. It is a component that is
// handed the requests that arrive at one address, and the server, the
// mux and the routes belong to the application that hosts it:
//
//	mux := http.NewServeMux()
//	mux.Handle("/logs/in", receiver)
//
// Whatever comes in through that URL is given to the receiver.
//
// What is here is the plainest thing that works: a packet as JSON in the
// body of a POST, and the confirmation as JSON in the answer. How the
// road is protected, and how the receiver is sure of who is at the other
// end, are part of the transport and have not been decided: nothing in
// this file should be taken as that decision.

// maxBody bounds the body of one request. Packets are small; this is a
// guard against something that is not a forwarder.
const maxBody = 8 << 20

type wirePacket struct {
	From   string      `json:"from,omitempty"`
	Log    string      `json:"log"`
	Chunks []wireChunk `json:"chunks"`
}

type wireChunk struct {
	S uint64 `json:"s"`
	T int64  `json:"t"`
	D []byte `json:"d"`
}

type wireAnswer struct {
	Confirmed uint64 `json:"confirmed"`
	Err       string `json:"err,omitempty"`
}

// ServeHTTP takes one packet per request. It makes a Receiver mountable
// on any mux:
//
//	mux.Handle("/logs/in", receiver)
//
// The answer carries the position up to which the log is confirmed, also
// when part of the packet could not be taken.
func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "a packet is sent with POST", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxBody))
	if err != nil {
		http.Error(w, "packet too large or cut short", http.StatusRequestEntityTooLarge)
		return
	}
	var wp wirePacket
	if err := json.Unmarshal(body, &wp); err != nil || wp.Log == "" {
		http.Error(w, "not a packet", http.StatusBadRequest)
		return
	}
	// Who sent it. The hosting application says, if it knows better than
	// the packet does: it is the one that authenticated the request.
	from := wp.From
	if r.cfg.Sender != nil {
		from, err = r.cfg.Sender(req)
		if err != nil {
			http.Error(w, "sender not accepted", http.StatusForbidden)
			return
		}
	}
	p := Packet{Log: wp.Log, Chunks: make([]qkernel.Chunk, len(wp.Chunks))}
	for i, c := range wp.Chunks {
		p.Chunks[i] = qkernel.Chunk{Seq: c.S, Time: time.Unix(0, c.T), Data: c.D}
	}
	confirmed, err := r.Accept(from, p)
	ans := wireAnswer{Confirmed: confirmed}
	status := http.StatusOK
	if err != nil {
		// Said in the answer rather than hidden: the sender keeps what
		// was not confirmed and tries again.
		ans.Err = err.Error()
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ans)
}

// HTTP returns a Transport that posts each packet to a URL where a
// Receiver is mounted. from is the name this sender gives itself; client
// may be nil for the default one.
//
// It is the counterpart of Receiver.ServeHTTP and as plain as it is. See
// the note at the top of this file: the protection of the road is not
// decided here.
func HTTP(url, from string, client *http.Client) Transport {
	if client == nil {
		client = http.DefaultClient
	}
	return TransportFunc(func(ctx context.Context, p Packet) (uint64, error) {
		wp := wirePacket{From: from, Log: p.Log, Chunks: make([]wireChunk, len(p.Chunks))}
		for i, c := range p.Chunks {
			d := c.Data
			if d == nil {
				d = []byte{}
			}
			wp.Chunks[i] = wireChunk{S: c.Seq, T: c.Time.UnixNano(), D: d}
		}
		body, err := json.Marshal(wp)
		if err != nil {
			return 0, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		var ans wireAnswer
		if json.Unmarshal(raw, &ans) != nil {
			return 0, fmt.Errorf("qforward: the other end answered %s", resp.Status)
		}
		if ans.Err != "" {
			return ans.Confirmed, fmt.Errorf("qforward: the other end: %s", ans.Err)
		}
		if resp.StatusCode != http.StatusOK {
			return ans.Confirmed, fmt.Errorf("qforward: the other end answered %s", resp.Status)
		}
		return ans.Confirmed, nil
	})
}
