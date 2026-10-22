# stack/

One subdirectory per supporting service the integration tests need that is
not defined inline in `../compose.instance.yml`. Each holds a `compose.yml`
fragment (listed in that file's `include:` list) and any config it
bind-mounts. No fragment declares `ports:` or `networks:`: services reach one
another by name on the compose network.

- `mqtt/` — Eclipse Mosquitto, the broker behind `tests/mqtt_events.rs`
  (the `mqtt` feature's real-broker tests). `mosquitto.conf` enables
  persistence and bounds the persistent-session queue, which the delivery
  guarantee in `src/events.rs` depends on. Host `mqtt`, port 1883, no auth
  or TLS (local development only).
