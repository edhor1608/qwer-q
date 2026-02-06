package broker

import (
	"sync"
	"time"
)

// DefaultHeartbeatTimeout is how long to wait before considering a group member dead.
const DefaultHeartbeatTimeout = 10 * time.Second

// groupMember represents a consumer within a group.
type groupMember struct {
	id            string // unique member ID (connection address)
	ch            chan *Message
	visTimeout    time.Duration
	lastHeartbeat time.Time
}

// ConsumerGroup tracks members consuming from a queue independently.
// Each group gets a copy of every message (fan-out across groups).
// Within a group, messages are delivered round-robin to members (competing consumers).
type ConsumerGroup struct {
	mu               sync.Mutex
	name             string
	queueName        string
	members          []*groupMember
	nextIdx          int
	messages         []*Message // pending messages for this group
	inFlight         map[string]*inFlightEntry
	heartbeatTimeout time.Duration
}

// inFlightEntry tracks which member has the message.
type inFlightEntry struct {
	msg      *Message
	memberID string
}

// NewConsumerGroup creates a new consumer group.
func NewConsumerGroup(name, queueName string) *ConsumerGroup {
	return &ConsumerGroup{
		name:             name,
		queueName:        queueName,
		inFlight:         make(map[string]*inFlightEntry),
		heartbeatTimeout: DefaultHeartbeatTimeout,
	}
}

// AddMember adds a consumer to the group. Returns the message channel.
func (g *ConsumerGroup) AddMember(memberID string, visTimeout time.Duration) <-chan *Message {
	ch := make(chan *Message, 1)
	m := &groupMember{
		id:            memberID,
		ch:            ch,
		visTimeout:    visTimeout,
		lastHeartbeat: time.Now(),
	}

	g.mu.Lock()
	g.members = append(g.members, m)
	g.tryDeliver()
	g.mu.Unlock()

	return ch
}

// RemoveMember removes a consumer from the group. Returns true if the group is now empty.
func (g *ConsumerGroup) RemoveMember(memberID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	for i, m := range g.members {
		if m.id == memberID {
			close(m.ch)
			g.members = append(g.members[:i], g.members[i+1:]...)
			if g.nextIdx >= len(g.members) && len(g.members) > 0 {
				g.nextIdx = 0
			}
			// Requeue any in-flight messages assigned to this member
			g.requeueForMember(memberID)
			break
		}
	}

	return len(g.members) == 0
}

// requeueForMember moves in-flight messages for a member back to pending. Must hold lock.
func (g *ConsumerGroup) requeueForMember(memberID string) {
	for id, entry := range g.inFlight {
		if entry.memberID == memberID {
			delete(g.inFlight, id)
			entry.msg.VisibleAt = time.Now()
			g.messages = append(g.messages, entry.msg)
		}
	}
	g.tryDeliver()
}

// Enqueue adds a message to this group's pending queue.
func (g *ConsumerGroup) Enqueue(msg *Message) {
	// Clone the message so each group has independent state.
	// Reset VisibleAt and Attempt: the queue's tryDeliver may have already
	// modified the original msg (setting VisibleAt to the future and incrementing
	// Attempt). Each group tracks its own delivery state independently.
	cloned := &Message{
		ID:          msg.ID,
		Queue:       msg.Queue,
		Payload:     msg.Payload,
		Headers:     msg.Headers,
		Attempt:     0,
		PublishedAt: msg.PublishedAt,
	}
	g.mu.Lock()
	g.messages = append(g.messages, cloned)
	g.tryDeliver()
	g.mu.Unlock()
}

// tryDeliver delivers messages to available members. Must hold lock.
func (g *ConsumerGroup) tryDeliver() {
	if len(g.members) == 0 || len(g.messages) == 0 {
		return
	}

	now := time.Now()
	for len(g.messages) > 0 {
		msg := g.messages[0]
		if msg.VisibleAt.After(now) {
			return
		}

		delivered := false
		for j := 0; j < len(g.members); j++ {
			idx := (g.nextIdx + j) % len(g.members)
			m := g.members[idx]

			select {
			case m.ch <- msg:
				copy(g.messages, g.messages[1:])
				g.messages = g.messages[:len(g.messages)-1]
				msg.Attempt++
				msg.VisibleAt = now.Add(m.visTimeout)
				g.inFlight[msg.ID] = &inFlightEntry{msg: msg, memberID: m.id}
				g.nextIdx = (idx + 1) % len(g.members)
				delivered = true
				break
			default:
			}
		}
		if !delivered {
			return
		}
	}
}

// Ack acknowledges a message within this group.
func (g *ConsumerGroup) Ack(messageID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.inFlight[messageID]; ok {
		delete(g.inFlight, messageID)
		g.tryDeliver()
		return true
	}
	return false
}

// Nack negatively acknowledges a message within this group.
// Returns a NackResult so the caller can handle DLQ if needed.
func (g *ConsumerGroup) Nack(messageID string, requeue bool, maxRetries uint32, policy FailurePolicy) NackResult {
	g.mu.Lock()
	defer g.mu.Unlock()

	entry, ok := g.inFlight[messageID]
	if !ok {
		return NackResult{Found: false}
	}
	delete(g.inFlight, messageID)
	msg := entry.msg

	if !requeue {
		if policy == FailurePolicyDLQ {
			return NackResult{Found: true, Message: msg, ToDLQ: true}
		}
		return NackResult{Found: true, Message: msg, ToDLQ: false}
	}

	// Check max retries
	if policy != FailurePolicyInfinite && msg.Attempt >= maxRetries {
		switch policy {
		case FailurePolicyDLQ:
			return NackResult{Found: true, Message: msg, ToDLQ: true}
		case FailurePolicyDrop:
			return NackResult{Found: true, Message: msg, ToDLQ: false}
		}
	}

	msg.VisibleAt = time.Now()
	g.messages = append(g.messages, msg)
	g.tryDeliver()
	return NackResult{Found: true, Message: msg, ToDLQ: false}
}

// Heartbeat updates the last heartbeat time for a member.
func (g *ConsumerGroup) Heartbeat(memberID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, m := range g.members {
		if m.id == memberID {
			m.lastHeartbeat = time.Now()
			return true
		}
	}
	return false
}

// ReapDeadMembers removes members that haven't sent a heartbeat within the timeout.
// Returns the IDs of removed members.
func (g *ConsumerGroup) ReapDeadMembers() []string {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	var dead []string
	alive := g.members[:0]

	for _, m := range g.members {
		if now.Sub(m.lastHeartbeat) > g.heartbeatTimeout {
			dead = append(dead, m.id)
			close(m.ch)
			g.requeueForMemberLocked(m.id)
		} else {
			alive = append(alive, m)
		}
	}

	g.members = alive
	if g.nextIdx >= len(g.members) && len(g.members) > 0 {
		g.nextIdx = 0
	}

	g.tryDeliver()
	return dead
}

// requeueForMemberLocked requeues in-flight messages for a member without calling tryDeliver.
// Used by ReapDeadMembers which does its own delivery pass after all dead members are processed.
func (g *ConsumerGroup) requeueForMemberLocked(memberID string) {
	for id, entry := range g.inFlight {
		if entry.memberID == memberID {
			delete(g.inFlight, id)
			entry.msg.VisibleAt = time.Now()
			g.messages = append(g.messages, entry.msg)
		}
	}
}

// RequeueExpired moves expired in-flight messages back to pending.
func (g *ConsumerGroup) RequeueExpired() {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	for id, entry := range g.inFlight {
		if entry.msg.VisibleAt.Before(now) {
			delete(g.inFlight, id)
			entry.msg.VisibleAt = now
			g.messages = append(g.messages, entry.msg)
		}
	}
	g.tryDeliver()
}

// Len returns the number of pending messages.
func (g *ConsumerGroup) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.messages)
}

// InFlightLen returns the number of in-flight messages.
func (g *ConsumerGroup) InFlightLen() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.inFlight)
}

// MemberCount returns the number of members.
func (g *ConsumerGroup) MemberCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.members)
}

// IsEmpty returns true if the group has no members.
func (g *ConsumerGroup) IsEmpty() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.members) == 0
}
