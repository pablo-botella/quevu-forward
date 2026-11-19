package qforward

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pablo-botella/quevu"
)

// Config sets up a Forwarder.
type Config struct {
	// Log is the name the log goes by at the other end.
	Log string
	// Spool is the path of the file where the queue is kept. A second
	// file, with ".sent" added, sits next to it. Both belong to this
	// forwarder alone.
	Spool string
	// Transport carries the packets.
	Transport Transport

	// MaxChunks and MaxBytes bound a packet: packets are small on
	// purpose. Defaults: 100 chunks, 64 KiB. A single chunk larger than
	// MaxBytes travels alone.
	MaxChunks int
	MaxBytes  int
	// Retry is how long to wait before sending again after a failure.
	// Default five seconds.
	Retry time.Duration
	// Timeout bounds one attempt to send a packet. Default one minute.
	Timeout time.Duration
	// TrimAt is the size at which the file of the queue is started over
	// once everything in it has been confirmed. Default 1 MiB.
	TrimAt int64
}

// Forwarder is a persistent consumer of a hub that delivers what it
// receives to a hub somewhere else.
//
// It takes a batch by writing it to its queue on disk, and that is all
// the hub waits for: the other end being slow, or down, never holds the
// hub back. Sending happens in the background, in order, and a chunk
// leaves the queue only when the other end has confirmed it.
type Forwarder struct {
	cfg   Config
	spool *spool

	wake chan struct{}
	stop context.CancelFunc
	done chan struct{}
	once sync.Once

	sent     atomic.Uint64
	failures atomic.Uint64
	mu       sync.Mutex
	lastErr  error
}

// Open opens the queue and begins sending what is in it. What a previous
// run left unconfirmed is sent first.
func Open(cfg Config) (*Forwarder, error) {
	switch {
	case cfg.Log == "":
		return nil, errors.New("qforward: no log name")
	case cfg.Spool == "":
		return nil, errors.New("qforward: no queue file")
	case cfg.Transport == nil:
		return nil, errors.New("qforward: no transport")
	}
	if cfg.MaxChunks <= 0 {
		cfg.MaxChunks = 100
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 64 << 10
	}
	if cfg.Retry <= 0 {
		cfg.Retry = 5 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = time.Minute
	}
	if cfg.TrimAt <= 0 {
		cfg.TrimAt = 1 << 20
	}
	sp, err := openSpool(cfg.Spool, cfg.TrimAt)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	f := &Forwarder{cfg: cfg, spool: sp, wake: make(chan struct{}, 1), stop: cancel, done: make(chan struct{})}
	go f.run(ctx)
	return f, nil
}

// Write takes a batch into the queue. When it returns without error the
// batch is on disk and will be delivered, now or after a restart.
func (f *Forwarder) Write(batch []quevu.Chunk) error {
	if err := f.spool.write(batch); err != nil {
		return err
	}
	select {
	case f.wake <- struct{}{}:
	default:
	}
	return nil
}

// LastSeq returns the last position this forwarder has taken, so the hub
// goes on counting from there after a restart.
func (f *Forwarder) LastSeq() (uint64, error) { return f.spool.lastSeq(), nil }

// Close stops sending and closes the queue. What has not been confirmed
// stays on disk for the next run: closing never waits for the other end.
func (f *Forwarder) Close() error {
	f.once.Do(func() {
		f.stop()
		<-f.done
	})
	return f.spool.close()
}

// Stats says how the forwarding is going.
type Stats struct {
	// Pending is how many chunks are in the queue waiting to be
	// confirmed by the other end.
	Pending uint64
	// Sent is how many chunks the other end has confirmed in this run.
	Sent uint64
	// Failures is how many attempts to send a packet have failed.
	Failures uint64
	// LastErr is the error of the last failed attempt, cleared by the
	// next success.
	LastErr error
}

// Stats returns how the forwarding is going. A Pending that keeps
// growing means the other end is not taking what is sent.
func (f *Forwarder) Stats() Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return Stats{Pending: f.spool.pending(), Sent: f.sent.Load(), Failures: f.failures.Load(), LastErr: f.lastErr}
}

// run sends the queue, packet by packet, for as long as the forwarder is
// open.
func (f *Forwarder) run(ctx context.Context) {
	defer close(f.done)
	for ctx.Err() == nil {
		p, err := f.packet()
		if err != nil {
			f.failed(err)
			f.sleep(ctx, f.cfg.Retry)
			continue
		}
		if len(p.Chunks) == 0 {
			// Nothing to send: wait for the hub to bring more.
			select {
			case <-ctx.Done():
			case <-f.wake:
			}
			continue
		}
		sctx, cancel := context.WithTimeout(ctx, f.cfg.Timeout)
		confirmed, err := f.cfg.Transport.Send(sctx, p)
		cancel()
		if err == nil && confirmed < p.Last() {
			err = errors.New("qforward: the other end did not confirm the whole packet")
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			f.failed(err)
			f.sleep(ctx, f.cfg.Retry)
			continue
		}
		// Confirmed: only now do these chunks leave the queue.
		if err := f.spool.confirm(p.Last()); err != nil {
			f.failed(err)
			f.sleep(ctx, f.cfg.Retry)
			continue
		}
		f.sent.Add(uint64(len(p.Chunks)))
		f.mu.Lock()
		f.lastErr = nil
		f.mu.Unlock()
	}
}

// packet takes the next chunks of the queue, as many as fit in a packet.
func (f *Forwarder) packet() (Packet, error) {
	chunks, err := f.spool.next(f.cfg.MaxChunks)
	if err != nil {
		return Packet{}, err
	}
	size := 0
	for i, c := range chunks {
		size += len(c.Data) + 32
		// At least one, even if it is too large on its own.
		if size > f.cfg.MaxBytes && i > 0 {
			chunks = chunks[:i]
			break
		}
	}
	return Packet{Log: f.cfg.Log, Chunks: chunks}, nil
}

func (f *Forwarder) failed(err error) {
	f.failures.Add(1)
	f.mu.Lock()
	f.lastErr = err
	f.mu.Unlock()
}

func (f *Forwarder) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

var (
	_ quevu.Consumer    = (*Forwarder)(nil)
	_ quevu.SeqConsumer = (*Forwarder)(nil)
)
