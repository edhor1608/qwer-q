package api

import (
	"encoding/json"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

// wsEvent is a real-time event sent to WebSocket clients.
type wsEvent struct {
	Type string `json:"type"` // "queue_depth", "consumer_change"
	Data any    `json:"data"`
}

type queueDepthData struct {
	Name     string `json:"name"`
	Depth    int    `json:"depth"`
	InFlight int    `json:"in_flight"`
}

type consumerChangeData struct {
	Name          string `json:"name"`
	ConsumerCount int    `json:"consumer_count"`
}

// wsHub manages WebSocket connections and broadcasts.
type wsHub struct {
	mu      sync.RWMutex
	clients map[*websocket.Conn]struct{}
}

func newWSHub() *wsHub {
	return &wsHub{clients: make(map[*websocket.Conn]struct{})}
}

func (hub *wsHub) add(conn *websocket.Conn) {
	hub.mu.Lock()
	hub.clients[conn] = struct{}{}
	hub.mu.Unlock()
}

func (hub *wsHub) remove(conn *websocket.Conn) {
	hub.mu.Lock()
	delete(hub.clients, conn)
	hub.mu.Unlock()
}

func (hub *wsHub) broadcast(event wsEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	for conn := range hub.clients {
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(data); err != nil {
			// Client will be removed on next read error
		}
	}
}

// startWSBroadcast periodically broadcasts queue state to connected clients.
func (h *Handler) startWSBroadcast() {
	ticker := time.NewTicker(time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-h.done:
				return
			case <-ticker.C:
			}
			h.hub.mu.RLock()
			count := len(h.hub.clients)
			h.hub.mu.RUnlock()
			if count == 0 {
				continue
			}

			names := h.broker.ListQueues()
			for _, name := range names {
				q := h.broker.GetQueue(name)
				if q == nil {
					continue
				}
				h.hub.broadcast(wsEvent{
					Type: "queue_depth",
					Data: queueDepthData{
						Name:     name,
						Depth:    q.Len(),
						InFlight: q.InFlightLen(),
					},
				})
			}
		}
	}()
}

func (h *Handler) handleWS(conn *websocket.Conn) {
	h.hub.add(conn)
	defer func() {
		h.hub.remove(conn)
		conn.Close()
	}()

	// Keep connection alive by reading (and discarding) client messages
	buf := make([]byte, 512)
	for {
		if _, err := conn.Read(buf); err != nil {
			return
		}
	}
}
