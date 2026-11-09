package pipeline

import (
	"bytes"
	"os"
	"sync"
)

// Event 推送给前端的事件（通过 SSE）。
type Event struct {
	Type string // release | log | end
	Data any
}

const maxHistory = 5000

// Hub 单次发布的事件广播中心：保存日志历史，新订阅者先回放再接收实时事件。
type Hub struct {
	mu      sync.Mutex
	logs    []string
	subs    map[chan Event]struct{}
	closed  bool
	logFile *os.File
	partial bytes.Buffer
}

func newHub(logPath string) *Hub {
	h := &Hub{subs: map[chan Event]struct{}{}}
	if f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
		h.logFile = f
	}
	return h
}

// Write 实现 io.Writer，按行切分（\n 或 \r）后广播。
func (h *Hub) Write(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, b := range p {
		if b == '\n' || b == '\r' {
			if h.partial.Len() > 0 {
				h.emitLineLocked(h.partial.String())
				h.partial.Reset()
			}
			continue
		}
		h.partial.WriteByte(b)
	}
	return len(p), nil
}

func (h *Hub) Logf(line string) {
	_, _ = h.Write([]byte(line + "\n"))
}

func (h *Hub) emitLineLocked(line string) {
	if h.logFile != nil {
		_, _ = h.logFile.WriteString(line + "\n")
	}
	h.logs = append(h.logs, line)
	if len(h.logs) > maxHistory {
		h.logs = h.logs[len(h.logs)-maxHistory:]
	}
	h.broadcastLocked(Event{Type: "log", Data: line})
}

func (h *Hub) Publish(e Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.broadcastLocked(e)
}

func (h *Hub) broadcastLocked(e Event) {
	for ch := range h.subs {
		select {
		case ch <- e:
		default: // 订阅者太慢，断开它（前端会自动重连并回放）
			delete(h.subs, ch)
			close(ch)
		}
	}
}

// Subscribe 返回历史日志与实时事件通道；hub 已关闭时 ch 为 nil。
func (h *Hub) Subscribe() (history []string, ch chan Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	history = append([]string(nil), h.logs...)
	if h.closed {
		return history, nil
	}
	ch = make(chan Event, 1024)
	h.subs[ch] = struct{}{}
	return history, ch
}

func (h *Hub) Unsubscribe(ch chan Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[ch]; ok {
		delete(h.subs, ch)
		close(ch)
	}
}

func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.partial.Len() > 0 {
		h.emitLineLocked(h.partial.String())
		h.partial.Reset()
	}
	h.broadcastLocked(Event{Type: "end"})
	for ch := range h.subs {
		close(ch)
	}
	h.subs = map[chan Event]struct{}{}
	h.closed = true
	if h.logFile != nil {
		_ = h.logFile.Close()
	}
}
