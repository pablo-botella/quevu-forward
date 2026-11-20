---
mkskill:
  pos: 100
  in: "*"
---

## The two ends

**Forwarder**, where the log is produced. It is a persistent writer of a
hub, and it guarantees its own queue:

- Taking a batch is writing it to disk. When `Write` returns, the batch
  will be delivered, now or after a restart. The hub is never held back by
  the other end being slow or down.
- Sending happens in the background, in order and in small packets
  (`MaxChunks`, `MaxBytes`).
- A chunk leaves the queue only when the other end has confirmed it.
- `Stats()` shows what is waiting and the last error. A `Pending` that
  keeps growing means the other end is not taking what is sent.

**Receiver**, where the log is wanted. It takes packets and feeds them
into hubs:

- `Into(from, log)` says which hub a log of a sender goes to. That is what
  keeps logs apart: two senders, or two logs of one sender, are two
  answers. Returning an error refuses the packet; the sender keeps it.
- A chunk is confirmed only once a hub has accepted it.
- A chunk keeps the time it had at its origin (if the hub is a
  `quevu.TimedIntake`, as a `quevu.Inlet` is). Its position is the next one of
  the hub it joins.

## At least once

A packet is sent again until the receiver confirms it, and a confirmation
can be lost on the way back, so the same packet may arrive twice. The
receiver remembers, for each log of each sender, the last position it
handed to a hub, and skips what it has already taken. With `State` set it
remembers across restarts.

What can still happen: the receiver's machine dies after a hub took some
chunks and before that was written down. Those chunks then arrive twice.
Nothing is lost in any case.

## The road between them

`Transport` is one method: send a packet, return the position the other
end confirms. Everything about the road belongs to it.

The receiver owns no web server. It is an `http.Handler`, mounted by the
hosting application on its own mux, at the URL it chooses.
`ReceiverConfig.Sender` lets that application say who a request comes
from, since it is the one that authenticated it; without it, the name the
packet gives itself is taken at its word.

`qforward.HTTP(url, from, client)` is the matching sender: a packet as
JSON in a POST, the confirmation as JSON in the answer.

## Rules and gotchas

- **Nothing here protects what travels.** See the note at the top.
- **One queue file per forwarder.** A second file, with `.sent` added,
  holds the position confirmed so far. Do not share or edit them.
- **Closing never waits for the other end.** What is unconfirmed stays on
  disk for the next run.
- **The queue file is started over** once everything in it has been
  confirmed and it has grown past `TrimAt`. While the other end is down it
  grows without limit: there is no cap yet.
- **A chunk larger than `MaxBytes` travels alone**, in a packet of its
  own.
- **Positions belong to the origin.** The receiver uses them only to tell
  what it has seen; the receiving hub numbers its chunks itself.
