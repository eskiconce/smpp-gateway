package esme

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/eskiconce/smpp-gateway/internal/smpp"
)

func TestPublisherConsumer(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	_ = NewESMEPublisher(rdb)

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()

	got := make(chan string, 1)
	go func() {
		conn, _ := ln.Accept()
		if conn == nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		p, _ := readPDU(br)
		if p != nil && p.Header.ID == smpp.DeliverSM {
			f, _ := smpp.ParseDeliverSM(p.Body)
			got <- f.ShortMessage
		}
	}()

	deliver := func(source, dest, dlrText string) error {
		conn, _ := net.Dial("tcp", ln.Addr().String())
		if conn == nil {
			return fmt.Errorf("no connection")
		}
		defer conn.Close()
		dlr := smpp.NewDeliverSM(1, source, dest, dlrText)
		_, err := conn.Write(smpp.Encode(dlr))
		return err
	}

	consumer := NewESMEConsumer(rdb, map[string]DeliverFunc{
		"t1": deliver,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.Run(ctx)

	time.Sleep(50 * time.Millisecond)

	ev := DLRMessage{
		TenantID: "t1", MessageID: "m1", SmscMsgid: "smsc-1",
		Msisdn: "569123", State: "DELIVRD", SourceChannel: "smpp",
	}
	data, _ := json.Marshal(ev)
	rdb.Publish(ctx, "esme-dlr:t1", data)

	select {
	case text := <-got:
		if text == "" {
			t.Fatal("dlr text vacio")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deliver_sm no recibido")
	}
}
