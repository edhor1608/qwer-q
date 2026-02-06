package broker

import (
	"net"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func TestGroupCreationImplicit(t *testing.T) {
	q := NewQueue("test")

	// No groups initially
	if g := q.GetGroup("group-a"); g != nil {
		t.Fatal("expected nil for non-existent group")
	}

	// GetOrCreateGroup creates on first access
	g := q.GetOrCreateGroup("group-a")
	if g == nil {
		t.Fatal("expected group to be created")
	}

	// Returns same instance
	g2 := q.GetOrCreateGroup("group-a")
	if g != g2 {
		t.Fatal("expected same group instance")
	}
}

func TestGroupSingleMemberDelivery(t *testing.T) {
	q := NewQueue("test")
	g := q.GetOrCreateGroup("group-a")

	ch := g.AddMember("member-1", 30*time.Second)

	msg := &Message{ID: NewULID(), Queue: "test", Payload: []byte("hello")}
	q.Enqueue(msg)

	select {
	case received := <-ch:
		if received.ID != msg.ID {
			t.Fatalf("expected message ID %s, got %s", msg.ID, received.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestGroupMultiGroupIndependentDelivery(t *testing.T) {
	// Two groups on the same queue should each get ALL messages
	q := NewQueue("test")
	gA := q.GetOrCreateGroup("group-a")
	gB := q.GetOrCreateGroup("group-b")

	chA := gA.AddMember("member-a1", 30*time.Second)
	chB := gB.AddMember("member-b1", 30*time.Second)

	msg := &Message{ID: NewULID(), Queue: "test", Payload: []byte("shared")}
	q.Enqueue(msg)

	// Both groups should receive the message
	select {
	case received := <-chA:
		if received.ID != msg.ID {
			t.Fatalf("group-a: expected message ID %s, got %s", msg.ID, received.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("group-a: timeout waiting for message")
	}

	select {
	case received := <-chB:
		if received.ID != msg.ID {
			t.Fatalf("group-b: expected message ID %s, got %s", msg.ID, received.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("group-b: timeout waiting for message")
	}
}

func TestGroupWithinGroupRoundRobin(t *testing.T) {
	// Multiple members in one group should get round-robin delivery
	q := NewQueue("test")
	g := q.GetOrCreateGroup("group-a")

	ch1 := g.AddMember("member-1", 30*time.Second)
	ch2 := g.AddMember("member-2", 30*time.Second)

	// Publish 4 messages
	for i := 0; i < 4; i++ {
		msg := &Message{ID: NewULID(), Queue: "test", Payload: []byte("msg")}
		q.Enqueue(msg)
	}

	// Each member should get 2 messages (channel buffer is 1, so we need to receive and ack)
	received1 := 0
	received2 := 0

	for i := 0; i < 4; i++ {
		select {
		case m := <-ch1:
			received1++
			g.Ack(m.ID)
		case m := <-ch2:
			received2++
			g.Ack(m.ID)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for message %d (got %d from ch1, %d from ch2)", i, received1, received2)
		}
	}

	if received1+received2 != 4 {
		t.Fatalf("expected 4 total messages, got %d+%d=%d", received1, received2, received1+received2)
	}
	// Both should have received some messages (round-robin)
	if received1 == 0 || received2 == 0 {
		t.Fatalf("expected both members to receive messages, got %d and %d", received1, received2)
	}
}

func TestGroupAck(t *testing.T) {
	g := NewConsumerGroup("group-a", "test")
	ch := g.AddMember("member-1", 30*time.Second)

	msg := &Message{ID: NewULID(), Queue: "test", Payload: []byte("data")}
	g.Enqueue(msg)

	<-ch

	if g.InFlightLen() != 1 {
		t.Fatalf("expected 1 in-flight, got %d", g.InFlightLen())
	}

	if !g.Ack(msg.ID) {
		t.Fatal("ack failed")
	}

	if g.InFlightLen() != 0 {
		t.Fatalf("expected 0 in-flight after ack, got %d", g.InFlightLen())
	}
}

func TestGroupNack(t *testing.T) {
	g := NewConsumerGroup("group-a", "test")
	ch := g.AddMember("member-1", 30*time.Second)

	msg := &Message{ID: NewULID(), Queue: "test", Payload: []byte("data")}
	g.Enqueue(msg)

	<-ch

	// Nack with requeue
	result := g.Nack(msg.ID, true, DefaultMaxRetries, FailurePolicyDLQ)
	if !result.Found {
		t.Fatal("nack failed")
	}

	// Message should be redelivered
	select {
	case received := <-ch:
		if received.ID != msg.ID {
			t.Fatalf("expected same message, got different ID")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for redelivered message")
	}
}

func TestGroupRemoveMemberRequeuesMessages(t *testing.T) {
	g := NewConsumerGroup("group-a", "test")
	ch1 := g.AddMember("member-1", 30*time.Second)
	ch2 := g.AddMember("member-2", 30*time.Second)

	msg := &Message{ID: NewULID(), Queue: "test", Payload: []byte("data")}
	g.Enqueue(msg)

	// One member gets it
	var receivedBy string
	select {
	case <-ch1:
		receivedBy = "member-1"
	case <-ch2:
		receivedBy = "member-2"
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// Remove the member that has the message
	g.RemoveMember(receivedBy)

	// Other member should get the requeued message
	var otherCh <-chan *Message
	if receivedBy == "member-1" {
		otherCh = ch2
	} else {
		otherCh = ch1
	}

	select {
	case received := <-otherCh:
		if received.ID != msg.ID {
			t.Fatalf("expected requeued message ID %s, got %s", msg.ID, received.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for requeued message")
	}
}

func TestGroupHeartbeat(t *testing.T) {
	g := NewConsumerGroup("group-a", "test")
	g.AddMember("member-1", 30*time.Second)

	if !g.Heartbeat("member-1") {
		t.Fatal("heartbeat for existing member should succeed")
	}

	if g.Heartbeat("non-existent") {
		t.Fatal("heartbeat for non-existent member should fail")
	}
}

func TestGroupHeartbeatTimeout(t *testing.T) {
	g := NewConsumerGroup("group-a", "test")
	g.heartbeatTimeout = 50 * time.Millisecond // Short timeout for testing

	ch := g.AddMember("member-1", 30*time.Second)

	msg := &Message{ID: NewULID(), Queue: "test", Payload: []byte("data")}
	g.Enqueue(msg)
	<-ch

	// Wait for heartbeat to expire
	time.Sleep(100 * time.Millisecond)

	dead := g.ReapDeadMembers()
	if len(dead) != 1 || dead[0] != "member-1" {
		t.Fatalf("expected member-1 to be reaped, got %v", dead)
	}

	if g.MemberCount() != 0 {
		t.Fatal("expected no members after reaping")
	}

	// In-flight message should be requeued to group's pending
	if g.Len() != 1 {
		t.Fatalf("expected 1 pending message after reaping, got %d", g.Len())
	}
}

func TestGroupHeartbeatKeepsAlive(t *testing.T) {
	g := NewConsumerGroup("group-a", "test")
	g.heartbeatTimeout = 100 * time.Millisecond

	g.AddMember("member-1", 30*time.Second)

	// Send heartbeats to keep alive
	time.Sleep(60 * time.Millisecond)
	g.Heartbeat("member-1")
	time.Sleep(60 * time.Millisecond)
	g.Heartbeat("member-1")
	time.Sleep(60 * time.Millisecond)

	dead := g.ReapDeadMembers()
	if len(dead) != 0 {
		t.Fatalf("member should be alive, got dead: %v", dead)
	}
}

func TestGroupLegacyConsumerNotAffected(t *testing.T) {
	// Non-grouped consumers should work exactly as before
	q := NewQueue("test")

	// Add a group and a legacy consumer
	g := q.GetOrCreateGroup("group-a")
	gCh := g.AddMember("member-g", 30*time.Second)
	legacyCh := q.Dequeue(30 * time.Second)

	msg := &Message{ID: NewULID(), Queue: "test", Payload: []byte("data")}
	q.Enqueue(msg)

	// Legacy consumer should get the message from the queue
	select {
	case received := <-legacyCh:
		if received.ID != msg.ID {
			t.Fatalf("legacy: expected message ID %s, got %s", msg.ID, received.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("legacy: timeout waiting for message")
	}

	// Group should also get its own copy
	select {
	case received := <-gCh:
		if received.ID != msg.ID {
			t.Fatalf("group: expected message ID %s, got %s", msg.ID, received.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("group: timeout waiting for message")
	}
}

func TestGroupRemoveEmptyGroup(t *testing.T) {
	q := NewQueue("test")
	g := q.GetOrCreateGroup("group-a")
	g.AddMember("member-1", 30*time.Second)

	// Remove the only member
	empty := g.RemoveMember("member-1")
	if !empty {
		t.Fatal("expected group to be empty after removing only member")
	}

	// Clean up
	q.RemoveGroup("group-a")
	if q.GetGroup("group-a") != nil {
		t.Fatal("expected group to be removed")
	}
}

func TestGroupRequeueExpired(t *testing.T) {
	g := NewConsumerGroup("group-a", "test")
	ch := g.AddMember("member-1", 50*time.Millisecond) // Very short visibility

	msg := &Message{ID: NewULID(), Queue: "test", Payload: []byte("data")}
	g.Enqueue(msg)
	<-ch

	if g.InFlightLen() != 1 {
		t.Fatalf("expected 1 in-flight, got %d", g.InFlightLen())
	}

	// Wait for visibility to expire
	time.Sleep(100 * time.Millisecond)
	g.RequeueExpired()

	// After RequeueExpired, the message is moved from inFlight to pending,
	// then tryDeliver immediately re-delivers it (channel is empty, buffer=1),
	// so it goes back to inFlight. Verify by reading the redelivered message.
	select {
	case received := <-ch:
		if received.ID != msg.ID {
			t.Fatalf("expected same message, got different ID")
		}
		// Attempt should have incremented again (was 1 from first delivery, now 2)
		if received.Attempt < 2 {
			t.Fatalf("expected attempt >= 2, got %d", received.Attempt)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for redelivered message")
	}
}

func TestGroupIntegrationViaServer(t *testing.T) {
	// Full integration: two connections in different groups, both get the message
	b := NewBroker()
	defer b.Close()

	server := NewServer(b)
	go server.ListenAndServe("127.0.0.1:0")
	defer server.Close()

	time.Sleep(50 * time.Millisecond)
	addr := server.Addr()
	if addr == nil {
		t.Fatal("server failed to start")
	}

	// Connect two consumers in different groups
	conn1, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("conn1: %v", err)
	}
	defer conn1.Close()

	conn2, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("conn2: %v", err)
	}
	defer conn2.Close()

	// Subscribe conn1 to group-a
	groupA := "group-a"
	consumeA := &protocol.ConsumeRequest{Queue: "test-queue", VisibilityTimeout: 30, Group: &groupA}
	dataA, _ := proto.Marshal(consumeA)
	conn1.Write(protocol.EncodeFrame(protocol.OpConsume, dataA))

	// Subscribe conn2 to group-b
	groupB := "group-b"
	consumeB := &protocol.ConsumeRequest{Queue: "test-queue", VisibilityTimeout: 30, Group: &groupB}
	dataB, _ := proto.Marshal(consumeB)
	conn2.Write(protocol.EncodeFrame(protocol.OpConsume, dataB))

	time.Sleep(50 * time.Millisecond)

	// Connect producer
	prodConn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer prodConn.Close()

	// Publish a message
	pubReq := &protocol.PublishRequest{Queue: "test-queue", Payload: []byte("group-test")}
	pubData, _ := proto.Marshal(pubReq)
	prodConn.Write(protocol.EncodeFrame(protocol.OpPublish, pubData))

	// Read publish ack
	frame, err := protocol.DecodeFrame(prodConn)
	if err != nil {
		t.Fatalf("failed to read publish ack: %v", err)
	}
	if frame.OpCode != protocol.OpPublishAck {
		t.Fatalf("expected OpPublishAck, got %v", frame.OpCode)
	}

	// Both consumers should get the message
	frame1, err := protocol.DecodeFrame(conn1)
	if err != nil {
		t.Fatalf("conn1 read: %v", err)
	}
	if frame1.OpCode != protocol.OpMessage {
		t.Fatalf("conn1: expected OpMessage, got %v", frame1.OpCode)
	}
	var msg1 protocol.Message
	proto.Unmarshal(frame1.Payload, &msg1)

	frame2, err := protocol.DecodeFrame(conn2)
	if err != nil {
		t.Fatalf("conn2 read: %v", err)
	}
	if frame2.OpCode != protocol.OpMessage {
		t.Fatalf("conn2: expected OpMessage, got %v", frame2.OpCode)
	}
	var msg2 protocol.Message
	proto.Unmarshal(frame2.Payload, &msg2)

	// Both should have the same message ID
	if msg1.MessageId != msg2.MessageId {
		t.Fatalf("both groups should get same message, got %s and %s", msg1.MessageId, msg2.MessageId)
	}
	if string(msg1.Payload) != "group-test" {
		t.Fatalf("wrong payload: %s", string(msg1.Payload))
	}
}

func TestHeartbeatViaServer(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	server := NewServer(b)
	go server.ListenAndServe("127.0.0.1:0")
	defer server.Close()

	time.Sleep(50 * time.Millisecond)
	addr := server.Addr()

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Subscribe to group
	groupName := "hb-group"
	consumeReq := &protocol.ConsumeRequest{Queue: "hb-queue", VisibilityTimeout: 30, Group: &groupName}
	consumeData, _ := proto.Marshal(consumeReq)
	conn.Write(protocol.EncodeFrame(protocol.OpConsume, consumeData))

	time.Sleep(50 * time.Millisecond)

	// Send heartbeat
	hbReq := &protocol.HeartbeatRequest{Group: "hb-group", Queue: "hb-queue"}
	hbData, _ := proto.Marshal(hbReq)
	conn.Write(protocol.EncodeFrame(protocol.OpHeartbeat, hbData))

	// Read heartbeat ack
	frame, err := protocol.DecodeFrame(conn)
	if err != nil {
		t.Fatalf("failed to read heartbeat ack: %v", err)
	}
	if frame.OpCode != protocol.OpHeartbeatAck {
		t.Fatalf("expected OpHeartbeatAck, got %v", frame.OpCode)
	}
}

func TestUnsubscribeViaServer(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	server := NewServer(b)
	go server.ListenAndServe("127.0.0.1:0")
	defer server.Close()

	time.Sleep(50 * time.Millisecond)
	addr := server.Addr()

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Subscribe to group
	groupName := "unsub-group"
	consumeReq := &protocol.ConsumeRequest{Queue: "unsub-queue", VisibilityTimeout: 30, Group: &groupName}
	consumeData, _ := proto.Marshal(consumeReq)
	conn.Write(protocol.EncodeFrame(protocol.OpConsume, consumeData))

	time.Sleep(50 * time.Millisecond)

	// Verify group exists with member
	q := b.GetQueue("unsub-queue")
	g := q.GetGroup("unsub-group")
	if g == nil {
		t.Fatal("expected group to exist")
	}
	if g.MemberCount() != 1 {
		t.Fatalf("expected 1 member, got %d", g.MemberCount())
	}

	// Unsubscribe
	unsubReq := &protocol.UnsubscribeRequest{Queue: "unsub-queue", Group: &groupName}
	unsubData, _ := proto.Marshal(unsubReq)
	conn.Write(protocol.EncodeFrame(protocol.OpUnsubscribe, unsubData))

	time.Sleep(50 * time.Millisecond)

	// Group should be removed (empty)
	if q.GetGroup("unsub-group") != nil {
		t.Fatal("expected group to be removed after last member unsubscribes")
	}
}

func TestGroupMultipleMessagesMultipleGroups(t *testing.T) {
	q := NewQueue("test")
	gA := q.GetOrCreateGroup("group-a")
	gB := q.GetOrCreateGroup("group-b")

	chA := gA.AddMember("a1", 30*time.Second)
	chB := gB.AddMember("b1", 30*time.Second)

	// Publish 3 messages
	ids := make([]string, 3)
	for i := 0; i < 3; i++ {
		msg := &Message{ID: NewULID(), Queue: "test", Payload: []byte("msg")}
		ids[i] = msg.ID
		q.Enqueue(msg)
	}

	// Both groups should get all 3 messages
	for i := 0; i < 3; i++ {
		select {
		case m := <-chA:
			if m.ID != ids[i] {
				t.Fatalf("group-a msg %d: expected %s, got %s", i, ids[i], m.ID)
			}
			gA.Ack(m.ID)
		case <-time.After(time.Second):
			t.Fatalf("group-a: timeout on msg %d", i)
		}
	}

	for i := 0; i < 3; i++ {
		select {
		case m := <-chB:
			if m.ID != ids[i] {
				t.Fatalf("group-b msg %d: expected %s, got %s", i, ids[i], m.ID)
			}
			gB.Ack(m.ID)
		case <-time.After(time.Second):
			t.Fatalf("group-b: timeout on msg %d", i)
		}
	}
}
