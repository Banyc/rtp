# `rtp`

A userspace reliable transmission protocol whose congestion window is governed only by the delivery rate (i.e., bandwidth/throughput) and the data loss rate.

## Performance is part of the contract

Bulk-lane throughput and interactive-lane latency are correctness properties of
this protocol, not tunables. A change that fixes a stall, a loss bug, or any
other defect by narrowing the congestion window, slowing the pacer, or otherwise
sending less is a regression even when every test passes: do not cap the window
below what the path delivers, and do not add latency to the interactive path to
buy reliability for the bulk path. The congestion window is flow-control bounded
by the peer's receive window -- which must therefore be large enough that the
bound does not fall below what the path delivers -- never arbitrarily clamped.
Any fix must be validated against the bulk-throughput and interactive-latency
scenarios, not only the correctness suite.

## TCP-compatible transport semantics

Treat RTP like TCP for behavior common to reliable stream transports. In particular, FIN is a graceful directional close and KILL/RST is an abort; EOF, half-close, and write-after-close behavior should follow the TCP model. When RTP intentionally defines different behavior, follow the RTP rule for that specific behavior and keep the difference inside the RTP implementation.

## Features

- an async-free reliable layer
  - a piece of pure algorithm
  - main entry: [[src/reliable/reliable_layer.rs]]
  - send window: [[src/traffic_shaping/recovery/pkt_send_space.rs]]
  - recv window: [[src/recv_queue/pkt_recv_space.rs]]
  - ACK calculation: [[src/ack/]] (intervals, wire selection, sender interpretation)
- a dead simple codec for packet encoding/decoding
  - main entry: [[src/codec.rs]]
  - wireshark dissector: [[wireshark/rtp.dissector.lua]]
- an async-based I/O-agnostic transmission layer
  - gluing the unreliable layer and the reliable layer together
  - main entry: [[src/transmission/transmission_layer.rs]]
- a user-facing I/O-agnostic socket wrapper
  - managing opening, closing, timer, async read/write for the transmission layer
  - perk: You are allowed to wait until or check if the send buf is empty!
  - owning the validated frame-delivery-to-AsyncRead/AsyncWrite adaptation
  - main entry: [[src/socket.rs]]
- a user-facing over-UDP implementation
  - exposing listening, accepting, and connecting APIs
  - main entry: [[src/udp.rs]]
- a user-facing keyed-streams over-single-UDP-connection implementation
  - exposing listening, accepting, and connecting APIs
  - main entry: [[src/keyed_udp.rs]]
- traffic-shaping policy, organized by concern:
  - adjacent observations/configuration: [[src/traffic_shaping/adjacent/]]
  - reverse/control traffic: [[src/traffic_shaping/control/]]
  - core forward shaping: [[src/traffic_shaping/core/]]
  - recovery shaping: [[src/traffic_shaping/recovery/]]
  - redundancy shaping: [[src/traffic_shaping/redundancy/]]

## How to use

Run the test suite:

```bash
cargo test
```

- as a high-level socket user: Refer to the [[examples]] directory and the test sections in [[src/udp.rs]] and [[src/keyed_udp.rs]].
- as a low-level reliable layer user: Refer to the test sections in [[src/socket.rs]].
