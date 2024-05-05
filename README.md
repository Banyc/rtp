# `rtp`

A userspace reliable transport protocol whose congestion window is governed only by the delivery rate (i.e., bandwidth/throughput) and the data loss rate.

## Features

- an async-free reliable layer
  - a piece of pure algorithm
  - main entry: [[src/reliable_layer.rs]]
  - send window: [[src/packet_send_space.rs]]
  - recv window: [[src/packet_recv_space.rs]]
  - send rate limiter: [[src/token_bucket.rs]]
  - SACK managing: [[src/sack.rs]]
- a dead simple codec for packet encoding/decoding
  - main entry: [[src/codec.rs]]
  - wireshark dissector: [[wireshark/rtp.dissector.lua]]
- an async-based I/O-agnostic transport layer
  - gluing the unreliable layer and the reliable layer together
  - main entry: [[src/transport_layer.rs]]
- a user-facing I/O-agnostic socket wrapper
  - managing closing, timer, async read/write for the transport layer
  - perk: You are allowed to wait until or check if the send buf is empty!
  - main entry: [[src/socket.rs]]
- a user-facing over-UDP implementation
  - exposing listening, accepting, and connecting APIs
  - main entry: [[src/udp.rs]]
- a user-facing keyed-streams over-single-UDP-connection implementation
  - exposing listening, accepting, and connecting APIs
  - main entry: [[src/keyed_udp.rs]]

## How to use

- as a high-level socket user: Refer to [[examples]] directory and the test sections in [[src/udp.rs]] and [[src/keyed_udp.rs]].
- as a low-level reliable layer user: Refer to the test sections in [[src/socket.rs]].
