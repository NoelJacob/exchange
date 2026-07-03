### Description
This is a FIX 4.2 and Websocket exchange binary. It can handle limit, market orders with GTC time set permanently and Self-trade prevention set permanently for all orders. Cancel orders are not implemented for now.

It uses correct FIX 4.2 protocol and a custom WS format like FIX. Once started it receives FIX at 9090 port and WS at 8080 port.

It can handle multiple clients and uses the same orderbook for WS and FIX orders. The orderbook has no ticker symbols by default and symbols are created on the fly when orders come in, so you can name your symbols anything while sending requests.

### FIX and WS Schema
The schema for FIX and WS are in `./FIX42.md` and `./WS.md` file. The FIX is standard industry schema and WS is a custom schema created by me inspired by FIX 4.2 but minimizing redundant arguments.

### Ports
```
FIX :9090
WS  :8080
```

### Running
__Requirements: Docker, Rust, Make__
- `make start`: Builds and runs it in a container with ports open
- `make start-local`: Uses the local Rust build from target folder and runs it
- `make stop`: Stops container
- `make clean`: Stops and cleans up containers

### Rust features
Enable the following features to emulate a wrongly matching exchange binary:
- prefilled: has prefilled orderbook with few limit orders already in the symbol BENCH
- panic_10s: panics after 10 seconds and binary crashes
- slow_submit: has a 100ms gap between the submit function taking inputs
- randomize_price: increases the price by 10% up or down randomly