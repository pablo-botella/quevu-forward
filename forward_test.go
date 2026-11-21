package qforward

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pablo-botella/quevu"
)

// hub stands for a hub at the receiving end: it keeps what it accepts,
// with the time it was told.
type hub struct {
	mu     sync.Mutex
	got    []string
	times  []time.Time
	refuse string
}

func (h *hub) Write(p []byte) (int, error) { return len(p), nil }

func (h *hub) Append(data []byte) error { return h.AppendAt(data, time.Now()) }

func (h *hub) AppendAt(data []byte, at time.Time) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.refuse != "" && string(data) == h.refuse {
		return errors.New("hub closed")
	}
	h.got = append(h.got, string(data))
	h.times = append(h.times, at)
	return nil
}

func (h *hub) all() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.got...)
}

func (h *hub) text() string { return strings.Join(h.all(), ",") }

// road joins a forwarder to a receiver directly, and can be told to
// misbehave the way a real one does.
type road struct {
	r    *Receiver
	from string

	mu        sync.Mutex
	down      bool
	loseReply int // confirmations to lose on the way back
	packets   [][]uint64
}

func (rd *road) Send(_ context.Context, p Packet) (uint64, error) {
	rd.mu.Lock()
	down := rd.down
	var seqs []uint64
	for _, c := range p.Chunks {
		seqs = append(seqs, c.Seq)
	}
	rd.packets = append(rd.packets, seqs)
	rd.mu.Unlock()
	if down {
		return 0, errors.New("the other end is not answering")
	}
	confirmed, err := rd.r.Accept(rd.from, p)
	rd.mu.Lock()
	defer rd.mu.Unlock()
	if rd.loseReply > 0 {
		rd.loseReply--
		return 0, errors.New("the confirmation was lost on the way back")
	}
	return confirmed, err
}

func (rd *road) set(down bool) {
	rd.mu.Lock()
	rd.down = down
	rd.mu.Unlock()
}

func (rd *road) sizes() []int {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	out := make([]int, len(rd.packets))
	for i, p := range rd.packets {
		out[i] = len(p)
	}
	return out
}

func chunks(from, to uint64) []quevu.Chunk {
	var out []quevu.Chunk
	for s := from; s <= to; s++ {
		out = append(out, quevu.Chunk{Seq: s, Time: time.Unix(1700000000, int64(s)), Data: fmt.Appendf(nil, "l%d", s)})
	}
	return out
}

func lines(from, to int) string {
	var out []string
	for i := from; i <= to; i++ {
		out = append(out, fmt.Sprintf("l%d", i))
	}
	return strings.Join(out, ",")
}

func wait(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !ok(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("never happened: " + what)
		}
	}
}

// ends builds a receiver that feeds one hub, and the road to it.
func ends(t *testing.T) (*hub, *Receiver, *road, string) {
	t.Helper()
	dir := t.TempDir()
	h := &hub{}
	r, err := NewReceiver(ReceiverConfig{
		Into:  func(from, log string) (quevu.Intake, error) { return h, nil },
		State: filepath.Join(dir, "receiver.state"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return h, r, &road{r: r, from: "app"}, dir
}

func open(t *testing.T, dir string, tr Transport, tune func(*Config)) *Forwarder {
	t.Helper()
	cfg := Config{Log: "errors", Spool: filepath.Join(dir, "queue.log"), Transport: tr, Retry: 10 * time.Millisecond}
	if tune != nil {
		tune(&cfg)
	}
	f, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestWhatAHubDistributesArrivesAtTheOtherEnd(t *testing.T) {
	h, _, rd, dir := ends(t)
	f := open(t, dir, rd, nil)
	defer f.Close()
	if err := f.Write(chunks(1, 5)); err != nil {
		t.Fatal(err)
	}
	wait(t, "five lines at the other end", func() bool { return h.text() == lines(1, 5) })
	wait(t, "the queue emptied", func() bool { return f.Stats().Pending == 0 })
	// A chunk keeps the time it had where it was written.
	h.mu.Lock()
	at := h.times[2]
	h.mu.Unlock()
	if !at.Equal(time.Unix(1700000000, 3)) {
		t.Fatalf("time at the other end: %v", at)
	}
	if st := f.Stats(); st.Sent != 5 || st.LastErr != nil {
		t.Fatalf("stats %+v", st)
	}
	if last, _ := f.LastSeq(); last != 5 {
		t.Fatalf("last %d", last)
	}
}

func TestPacketsAreSmall(t *testing.T) {
	h, _, rd, dir := ends(t)
	f := open(t, dir, rd, func(c *Config) { c.MaxChunks = 10 })
	defer f.Close()
	rd.set(true)
	f.Write(chunks(1, 35))
	// Sent in one go, they would be one packet. They are four.
	rd.mu.Lock()
	rd.packets = nil
	rd.mu.Unlock()
	rd.set(false)
	wait(t, "35 lines", func() bool { return h.text() == lines(1, 35) })
	if got := fmt.Sprint(rd.sizes()); got != "[10 10 10 5]" {
		t.Fatalf("packets: %s", got)
	}
}

func TestAPacketIsBoundedInBytesToo(t *testing.T) {
	h, _, rd, dir := ends(t)
	f := open(t, dir, rd, func(c *Config) { c.MaxBytes = 1000 })
	defer f.Close()
	rd.set(true)
	big := strings.Repeat("x", 400)
	var batch []quevu.Chunk
	for s := uint64(1); s <= 5; s++ {
		batch = append(batch, quevu.Chunk{Seq: s, Time: time.Now(), Data: []byte(big)})
	}
	// And one that is too large for any packet: it travels alone.
	batch = append(batch, quevu.Chunk{Seq: 6, Time: time.Now(), Data: []byte(strings.Repeat("y", 5000))})
	f.Write(batch)
	rd.mu.Lock()
	rd.packets = nil
	rd.mu.Unlock()
	rd.set(false)
	wait(t, "six chunks", func() bool { return len(h.all()) == 6 })
	if got := fmt.Sprint(rd.sizes()); got != "[2 2 1 1]" {
		t.Fatalf("packets: %s", got)
	}
}

func TestTheOtherEndBeingDownLosesNothing(t *testing.T) {
	h, _, rd, dir := ends(t)
	rd.set(true)
	f := open(t, dir, rd, nil)
	defer f.Close()
	// The hub is never held back: taking a batch is writing it to disk.
	began := time.Now()
	for s := uint64(1); s <= 50; s++ {
		if err := f.Write(chunks(s, s)); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(began); d > 3*time.Second {
		t.Fatalf("writes took %v with the other end down", d)
	}
	wait(t, "failures noticed", func() bool { return f.Stats().Failures > 0 })
	if st := f.Stats(); st.Pending != 50 || st.LastErr == nil || len(h.all()) != 0 {
		t.Fatalf("stats %+v, other end has %d", st, len(h.all()))
	}
	rd.set(false)
	wait(t, "everything delivered, in order", func() bool { return h.text() == lines(1, 50) })
	wait(t, "the queue emptied", func() bool { return f.Stats().Pending == 0 && f.Stats().LastErr == nil })
}

func TestARestartLosesNothing(t *testing.T) {
	h, _, rd, dir := ends(t)
	rd.set(true)
	f := open(t, dir, rd, nil)
	f.Write(chunks(1, 20))
	// The application stops with the other end still down.
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if len(h.all()) != 0 {
		t.Fatal("something was delivered through a road that was down")
	}

	// It starts again. The hub counts on from where the queue was...
	f = open(t, dir, rd, nil)
	defer f.Close()
	if last, _ := f.LastSeq(); last != 20 {
		t.Fatalf("after restart, last %d", last)
	}
	if st := f.Stats(); st.Pending != 20 {
		t.Fatalf("after restart, pending %d", st.Pending)
	}
	// ...and what was waiting is delivered before what comes now.
	f.Write(chunks(21, 25))
	rd.set(false)
	wait(t, "everything, old first", func() bool { return h.text() == lines(1, 25) })
}

func TestARepeatedPacketIsTakenOnce(t *testing.T) {
	h, r, rd, dir := ends(t)
	// The other end takes the packet, but the forwarder never hears it.
	rd.loseReply = 3
	f := open(t, dir, rd, func(c *Config) { c.MaxChunks = 4 })
	defer f.Close()
	f.Write(chunks(1, 10))
	wait(t, "the queue emptied", func() bool { return f.Stats().Pending == 0 })
	// Packets were sent again; nothing arrived twice.
	if h.text() != lines(1, 10) {
		t.Fatalf("other end: %s", h.text())
	}
	if n := len(rd.sizes()); n <= 3 {
		t.Fatalf("only %d packets: nothing was repeated", n)
	}
	if r.Taken("app", "errors") != 10 {
		t.Fatalf("taken %d", r.Taken("app", "errors"))
	}
}

func TestTheReceiverRemembersAcrossARestart(t *testing.T) {
	h, r, _, dir := ends(t)
	p := Packet{Log: "errors", Chunks: chunks(1, 6)}
	if c, err := r.Accept("app", p); err != nil || c != 6 {
		t.Fatalf("confirmed %d, %v", c, err)
	}
	r.Close()
	// The server restarts, and the same packet comes again.
	r2, err := NewReceiver(ReceiverConfig{
		Into:  func(string, string) (quevu.Intake, error) { return h, nil },
		State: filepath.Join(dir, "receiver.state"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if c, err := r2.Accept("app", p); err != nil || c != 6 {
		t.Fatalf("confirmed %d, %v", c, err)
	}
	// With part of it new.
	if c, err := r2.Accept("app", Packet{Log: "errors", Chunks: chunks(5, 8)}); err != nil || c != 8 {
		t.Fatalf("confirmed %d, %v", c, err)
	}
	if h.text() != lines(1, 8) {
		t.Fatalf("hub: %s", h.text())
	}
}

func TestSeveralSendersAndLogsAreKeptApart(t *testing.T) {
	hubs := map[string]*hub{}
	var mu sync.Mutex
	r, _ := NewReceiver(ReceiverConfig{Into: func(from, log string) (quevu.Intake, error) {
		if from == "stranger" {
			return nil, errors.New("not expected here")
		}
		mu.Lock()
		defer mu.Unlock()
		k := from + "/" + log
		if hubs[k] == nil {
			hubs[k] = &hub{}
		}
		return hubs[k], nil
	}})
	// Two machines, each with a log called the same, and one with two.
	r.Accept("shop-1", Packet{Log: "errors", Chunks: chunks(1, 3)})
	r.Accept("shop-2", Packet{Log: "errors", Chunks: chunks(1, 2)})
	r.Accept("shop-1", Packet{Log: "audit", Chunks: chunks(1, 4)})
	// Each counts its positions by itself: position 1 of one is not
	// position 1 of another.
	for k, want := range map[string]string{"shop-1/errors": lines(1, 3), "shop-2/errors": lines(1, 2), "shop-1/audit": lines(1, 4)} {
		if hubs[k] == nil || hubs[k].text() != want {
			t.Fatalf("%s: %v", k, hubs[k])
		}
	}
	if r.Taken("shop-1", "errors") != 3 || r.Taken("shop-2", "errors") != 2 {
		t.Fatal("positions mixed up")
	}
	// A sender nobody expects is refused, and keeps its packet.
	if c, err := r.Accept("stranger", Packet{Log: "errors", Chunks: chunks(1, 2)}); err == nil || c != 0 {
		t.Fatalf("stranger: confirmed %d, %v", c, err)
	}
}

func TestWhatTheHubRefusesIsNotConfirmed(t *testing.T) {
	h, r, _, _ := ends(t)
	h.refuse = "l3"
	c, err := r.Accept("app", Packet{Log: "errors", Chunks: chunks(1, 5)})
	// The two before it are in the hub and confirmed; the rest is not.
	if err == nil || c != 2 || h.text() != lines(1, 2) {
		t.Fatalf("confirmed %d, %v, hub %s", c, err, h.text())
	}
	// The sender brings the packet again once the hub takes it.
	h.refuse = ""
	if c, err := r.Accept("app", Packet{Log: "errors", Chunks: chunks(1, 5)}); err != nil || c != 5 {
		t.Fatalf("confirmed %d, %v", c, err)
	}
	if h.text() != lines(1, 5) {
		t.Fatalf("hub: %s", h.text())
	}
}

func TestTheQueueFileDoesNotGrowForEver(t *testing.T) {
	h, _, rd, dir := ends(t)
	f := open(t, dir, rd, func(c *Config) { c.TrimAt = 2000 })
	defer f.Close()
	path := filepath.Join(dir, "queue.log")
	for s := uint64(1); s <= 600; s += 20 {
		f.Write(chunks(s, s+19))
		wait(t, "the batch confirmed", func() bool { return f.Stats().Pending == 0 })
	}
	if h.text() != lines(1, 600) {
		t.Fatal("not everything arrived, or not in order")
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// 600 lines would be some twenty kilobytes if nothing were dropped.
	if st.Size() > 6000 {
		t.Fatalf("the queue file is %d bytes", st.Size())
	}
	// Starting it over did not make the hub lose count.
	if last, _ := f.LastSeq(); last != 600 {
		t.Fatalf("last %d", last)
	}
}

func TestMountedOnAMuxOfTheHostingApplication(t *testing.T) {
	h := &hub{}
	r, _ := NewReceiver(ReceiverConfig{Into: func(from, log string) (quevu.Intake, error) {
		if log != "errors" {
			return nil, errors.New("no such log here")
		}
		return h, nil
	}})
	// The server, the mux and the route are the application's.
	mux := http.NewServeMux()
	mux.Handle("/logs/in", r)
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		hits.Add(1)
		mux.ServeHTTP(w, req)
	}))
	defer srv.Close()

	f := open(t, t.TempDir(), HTTP(srv.URL+"/logs/in", "shop-1", srv.Client()), func(c *Config) { c.MaxChunks = 5 })
	defer f.Close()
	in := chunks(1, 12)
	in[3].Data = []byte("\x00\xff not text")
	in[4].Data = nil
	f.Write(in)
	wait(t, "twelve chunks through the web server", func() bool { return len(h.all()) == 12 })
	if got := h.all(); got[3] != "\x00\xff not text" || got[4] != "" || got[11] != "l12" {
		t.Fatalf("hub: %q", got)
	}
	if r.Taken("shop-1", "errors") != 12 || hits.Load() < 3 {
		t.Fatalf("taken %d in %d requests", r.Taken("shop-1", "errors"), hits.Load())
	}

	// What is not a packet is turned away.
	if resp, _ := srv.Client().Get(srv.URL + "/logs/in"); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET: %s", resp.Status)
	}
	if resp, _ := srv.Client().Post(srv.URL+"/logs/in", "application/json", strings.NewReader("hello")); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("garbage: %s", resp.Status)
	}
	// A log the application has no place for is refused, and said.
	other := HTTP(srv.URL+"/logs/in", "shop-1", nil)
	if _, err := other.Send(context.Background(), Packet{Log: "audit", Chunks: chunks(1, 1)}); err == nil || !strings.Contains(err.Error(), "no such log") {
		t.Fatalf("got %v", err)
	}
}

func TestTheHostingApplicationSaysWhoIsCalling(t *testing.T) {
	h := &hub{}
	var seen []string
	r, _ := NewReceiver(ReceiverConfig{
		Into: func(from, log string) (quevu.Intake, error) { seen = append(seen, from); return h, nil },
		// However the application authenticated the request, it is the
		// one that knows. Here: a header a proxy in front would set.
		Sender: func(req *http.Request) (string, error) {
			who := req.Header.Get("X-Authenticated-As")
			if who == "" {
				return "", errors.New("nobody")
			}
			return who, nil
		},
	})
	srv := httptest.NewServer(r)
	defer srv.Close()

	// The packet calls itself "liar"; the application knows better.
	client := &http.Client{Transport: headers{"X-Authenticated-As": "shop-7"}}
	tr := HTTP(srv.URL, "liar", client)
	if c, err := tr.Send(context.Background(), Packet{Log: "errors", Chunks: chunks(1, 2)}); err != nil || c != 2 {
		t.Fatalf("confirmed %d, %v", c, err)
	}
	if len(seen) != 1 || seen[0] != "shop-7" {
		t.Fatalf("taken as %v", seen)
	}
	// Nobody the application recognizes: refused before anything else.
	if _, err := HTTP(srv.URL, "liar", nil).Send(context.Background(), Packet{Log: "errors", Chunks: chunks(3, 3)}); err == nil {
		t.Fatal("an unauthenticated request was taken")
	}
	if h.text() != lines(1, 2) {
		t.Fatalf("hub: %s", h.text())
	}
}

type headers map[string]string

func (h headers) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	for k, v := range h {
		req.Header.Set(k, v)
	}
	return http.DefaultTransport.RoundTrip(req)
}

func TestConfigIsChecked(t *testing.T) {
	tr := TransportFunc(func(context.Context, Packet) (uint64, error) { return 0, nil })
	for _, cfg := range []Config{{Spool: "q", Transport: tr}, {Log: "l", Transport: tr}, {Log: "l", Spool: "q"}} {
		if _, err := Open(cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
	if _, err := NewReceiver(ReceiverConfig{}); err == nil {
		t.Fatal("a receiver with nowhere to put things was accepted")
	}
	if (Packet{}).Last() != 0 {
		t.Fatal("last of an empty packet")
	}
}
