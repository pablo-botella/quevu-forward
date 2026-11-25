package qforward

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/pablo-botella/quevu/qkernel"
	"github.com/pablo-botella/quevu/qtxt"
)

// spool is the queue a forwarder guarantees: every chunk it has taken
// and not yet seen confirmed, on disk.
//
// It is two files. The chunks wait in a text file, written and read with
// quevu/qtxt, the same way a log is kept in a file anywhere else in quevu.
// Next to it a small file holds one number: the position up to which the
// other end has confirmed. What lies after that number is what is still
// to be sent; it is all that has to survive a restart.
type spool struct {
	path string
	trim int64

	mu     sync.Mutex
	dev    *qtxt.Device
	reader *qtxt.Reader
	// confirmed is the last position the other end has confirmed, and
	// last the last one written here.
	confirmed uint64
	last      uint64
}

func (s *spool) cursorPath() string { return s.path + ".sent" }

func openSpool(path string, trim int64) (*spool, error) {
	s := &spool{path: path, trim: trim, reader: qtxt.NewReader(path)}
	if b, err := os.ReadFile(s.cursorPath()); err == nil {
		n, perr := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
		if perr != nil {
			return nil, fmt.Errorf("qforward: %s is damaged: %w", s.cursorPath(), perr)
		}
		s.confirmed = n
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := s.open(); err != nil {
		return nil, err
	}
	return s, nil
}

// open opens the file of chunks. Every batch is forced to the disk
// before it counts as taken: that is the guarantee.
func (s *spool) open() error {
	dev, err := qtxt.Open(s.path, qtxt.Options{Sync: true})
	if err != nil {
		return err
	}
	s.dev = dev
	last, _ := dev.LastSeq()
	s.last = max(s.last, last, s.confirmed)
	return nil
}

// write takes a batch. When it returns without error the batch is on
// disk.
func (s *spool) write(batch []qkernel.Chunk) error {
	if len(batch) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dev == nil {
		return errors.New("qforward: queue closed")
	}
	if err := s.dev.Write(batch); err != nil {
		return err
	}
	s.last = batch[len(batch)-1].Seq
	return nil
}

// next returns up to n chunks that are still to be sent.
func (s *spool) next(n int) ([]qkernel.Chunk, error) {
	s.mu.Lock()
	from := s.confirmed
	s.mu.Unlock()
	return s.reader.After(from, n)
}

// confirm records that the other end has everything up to seq. Once
// nothing is left to send, a file that has grown is started over.
func (s *spool) confirm(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq <= s.confirmed {
		return nil
	}
	s.confirmed = seq
	// Written aside and moved into place, so a crash never leaves half a
	// number behind.
	tmp := s.cursorPath() + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(seq, 10)+"\n"), 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.cursorPath()); err != nil {
		return err
	}
	if s.dev != nil && s.confirmed >= s.last && s.trim > 0 {
		if st, err := os.Stat(s.path); err == nil && st.Size() > s.trim {
			// Everything in the file has been confirmed: none of it is
			// needed again. The number next to it remembers the
			// position, so the count goes on.
			s.dev.Close()
			s.dev = nil
			if err := os.Remove(s.path); err != nil {
				return err
			}
			return s.open()
		}
	}
	return nil
}

// pending returns how many chunks are still to be sent.
func (s *spool) pending() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last - s.confirmed
}

func (s *spool) lastSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func (s *spool) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dev == nil {
		return nil
	}
	err := s.dev.Close()
	s.dev = nil
	return err
}
