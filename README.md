# Streams Service PoC

A quick proof-of-concept implementation of the Streams service. See the [docs for this API](https://docs.google.com/document/d/1RHi12vctThbfFnrlS4sq8zhGwfPTXQ-NLj8BskFiYpI/edit).

## Getting Started

This service should work with minimal setup

1. Install gin
  * `go get github.com/codegangsta/gin`
2. Run with gin (ensure your pwd is this cloned repo)
  * `gin`
3. Test the server
  * `curl http://127.0.0.1:3000/streams`
