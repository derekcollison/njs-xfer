package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

func usage() {
	log.Printf("Usage: njs-xfer [-s server] [-creds file] <put|get> <file|stream>\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var creds = flag.String("creds", "", "User Credentials File")
	var showHelp = flag.Bool("h", false, "Show help message")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	args := flag.Args()
	if len(args) < 2 {
		showUsageAndExit(1)
	}

	cmd := strings.ToLower(args[0])
	if cmd != "put" && cmd != "get" {
		showUsageAndExit(1)
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS JetStream Transfer")}
	opts = setupConnOptions(opts)

	// Use UserCredentials
	if *creds != "" {
		opts = append(opts, nats.UserCredentials(*creds))
	}

	// Connect to NATS
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	switch cmd {
	case "put":
		putFile(nc, args[1])
	case "get":
		getFile(nc, args[1])
	}
}

func canonicalName(name string) string {
	fn := filepath.Base(filepath.Clean(name))
	fn = strings.ReplaceAll(fn, ".", "_")
	return strings.ReplaceAll(fn, " ", "_")
}

// putFile will place the file resource into a JetStream stream for later retrieval.
func putFile(nc *nats.Conn, fileName string) {
	// Make sure we have a legitimate file resource.
	fd, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Error opening %q: %v", fileName, err)
	}
	defer fd.Close()

	// We could grab metadata for the file resource here and either use the first message
	// to represent the metadata or as headers.

	// Create our jetstream context.
	// On an error we will just exit.
	errHandler := func(_ nats.JetStream, _ *nats.Msg, err error) {
		log.Fatalf("Error sending chunk to JetStream: %v", err)
	}
	// We will use a sliding window and async publishes to maximize performance.
	const maxPending = 8 // 8 * 64k
	js, err := nc.JetStream(
		nats.PublishAsyncMaxPending(maxPending),
		nats.PublishAsyncErrHandler(errHandler),
	)
	if err != nil {
		log.Fatalf("%v", err)
	}

	// We will use the filename as the stream name, but we need to replace "."
	stream := canonicalName(fileName)
	if _, err = js.StreamInfo(stream); err == nil {
		log.Fatalf("Stream %q already exists", stream)
	}
	// Delivery subject as an inbox to avoid accidentally interfering with other subjects.
	subj := nats.NewInbox()

	// Create our stream.
	// TODO(dlc) - Could add in replication as an argument.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: []string{subj},
	})
	if err != nil {
		log.Fatalf("Unexpected error creating stream: %v", err)
	}

	// Define our chunk size to be 64k. Important not to make this too big, NATS likes smaller messages
	// and is plenty fast to transfer at very high rates even with smaller payloads.
	const chunkSize = 64 * 1024
	chunk := make([]byte, chunkSize)

	// TODO(dlc) - Coould compress here if we wanted as well.

	// Loop and grab chunks from the file.
	start, bytes := time.Now(), 0
	for {
		n, err := fd.Read(chunk)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Error reading: %v", err)
		}
		if _, err = js.PublishAsync(subj, chunk[:n]); err != nil {
			log.Fatalf("Error sending chunk to JetStream: %v", err)
		}
		bytes += n
	}
	log.Printf("Completed transfer of %v in %v", friendlyBytes(bytes), time.Since(start))
}

// getFile will retrieve the file resource from the JetStream stream.
func getFile(nc *nats.Conn, fileName string) {
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("%v", err)
	}

	stream := canonicalName(fileName)
	si, err := js.StreamInfo(stream)
	if err != nil {
		log.Fatalf("Could not find stream: %s", stream)
	}

	if _, err := os.Stat(stream); !os.IsNotExist(err) {
		log.Fatalf("Destination file already exists: %s", stream)
	}

	fd, err := os.Create(stream)
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
	}
	defer fd.Close()

	// We have multiple options here with respect to configuring a consumer.
	// We care about not being a slow consumer and recovering from any dataloss or missed chunks.
	// We could do a replay controller rate, or max ack pending, or even a pull based consumer.
	// However with this scenario, we really do not need acks or redeliveries and can use the new
	// flowcontrol option to control bandwidth. We can use the consumer sequences to detect any missed
	// chunks.

	createSub := func(startSeq uint64) *nats.Subscription {
		sub, err := js.SubscribeSync(
			si.Config.Subjects[0],
			nats.AckNone(),
			nats.MaxDeliver(1),
			nats.StartSequence(startSeq),
			nats.EnableFlowControl(),
		)
		if err != nil {
			log.Fatalf("Error creating consumer: %v", err)
		}
		return sub
	}

	sub := createSub(1)
	defer sub.Unsubscribe()

	start := time.Now()
	bytes, last, eseq := 0, si.State.Msgs, uint64(1)

	// Loop over our inbound messages.
	for m, err := sub.NextMsg(5 * time.Second); err == nil; m, err = sub.NextMsg(time.Second) {
		meta, err := m.Metadata()
		if err != nil {
			log.Fatal(err)
		}
		if eseq != meta.Sequence.Stream {
			log.Printf("Missed chunk sequence, expected %d but got %d, resetting", eseq, meta.Sequence.Stream)
			sub = createSub(eseq)
			continue
		}

		// Write to our file.
		fd.Write(m.Data)
		bytes += len(m.Data)

		// Check to see if we are done.
		eseq++
		if eseq > last {
			break
		}
	}
	log.Printf("Completed retrieval of %v in %v", friendlyBytes(bytes), time.Since(start))
	fd.Close()
}

func friendlyBytes(bytes int) string {
	fbytes := float64(bytes)
	base := 1024
	pre := []string{"K", "M", "G", "T", "P", "E"}
	if fbytes < float64(base) {
		return fmt.Sprintf("%v B", fbytes)
	}
	exp := int(math.Log(fbytes) / math.Log(float64(base)))
	index := exp - 1
	return fmt.Sprintf("%.2f %sB", fbytes/math.Pow(float64(base), float64(exp)), pre[index])
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Second
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("Disconnected due to: %s, will attempt reconnects for %.0fs", err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatalf("Exiting: %v", nc.LastError())
	}))
	return opts
}
