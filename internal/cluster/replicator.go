package cluster

import (
	"encoding/json"
	"fmt"
	"time"
)

// defaultApplyTimeout is how long to wait for Raft consensus.
const defaultApplyTimeout = 5 * time.Second

// ReplicatePublish replicates a message publish through Raft consensus.
// The message is committed to a majority of nodes before returning.
func (n *Node) ReplicatePublish(queue, messageID string, payload []byte, headers map[string]string, publishedAt time.Time, stream bool) (string, error) {
	pubCmd := PublishCommand{
		Queue:       queue,
		MessageID:   messageID,
		Payload:     payload,
		Headers:     headers,
		PublishedAt: publishedAt.UnixMilli(),
		Stream:      stream,
	}

	data, err := json.Marshal(pubCmd)
	if err != nil {
		return "", fmt.Errorf("marshal publish command: %w", err)
	}

	cmd := Command{
		Type: CmdPublish,
		Data: data,
	}

	resp, err := n.Apply(cmd, defaultApplyTimeout)
	if err != nil {
		return "", err
	}

	return resp.MessageID, nil
}

// ReplicateAck replicates a message ack through Raft consensus.
func (n *Node) ReplicateAck(queue, messageID string) error {
	ackCmd := AckCommand{
		Queue:     queue,
		MessageID: messageID,
	}

	data, err := json.Marshal(ackCmd)
	if err != nil {
		return fmt.Errorf("marshal ack command: %w", err)
	}

	cmd := Command{
		Type: CmdAck,
		Data: data,
	}

	_, err = n.Apply(cmd, defaultApplyTimeout)
	return err
}

// ReplicateNack replicates a message nack through Raft consensus.
func (n *Node) ReplicateNack(queue, messageID string, requeue bool) error {
	nackCmd := NackCommand{
		Queue:     queue,
		MessageID: messageID,
		Requeue:   requeue,
	}

	data, err := json.Marshal(nackCmd)
	if err != nil {
		return fmt.Errorf("marshal nack command: %w", err)
	}

	cmd := Command{
		Type: CmdNack,
		Data: data,
	}

	_, err = n.Apply(cmd, defaultApplyTimeout)
	return err
}
