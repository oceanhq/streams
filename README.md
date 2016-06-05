# Streams Service PoC

A quick proof-of-concept implementation of the Streams service. See the [docs for this API](https://docs.google.com/document/d/1RHi12vctThbfFnrlS4sq8zhGwfPTXQ-NLj8BskFiYpI/edit).

## Getting Started

This service should work with minimal setup if you already have your Go environment set up. If not, see the [Go Getting Started guide](https://golang.org/doc/install).

1. Install gin
  * `go get github.com/codegangsta/gin`
2. Run with gin (ensure your pwd is this cloned repo)
  * `gin`
3. Test the server
  * `curl http://127.0.0.1:3000/streams`

## Running on SNS/SQS

To run on SQS/SNS you'll need to provide a valid AWS account #, Access Key ID, and Secret Access Key in your .env.

Additionally, the app expects two DynamoDB tables on that account:

- ocean-streams
- ocean-cursors

Finally, you'll want to build with the `sqs` tag. To manage this, I recommend using my fork of gin.

```sh
go get github.com/tobyjsullivan/gin
go install github.com/tobyjsullivan/gin
gin -tags sqs
```

