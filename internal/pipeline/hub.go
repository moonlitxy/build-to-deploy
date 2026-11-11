package pipeline

import (
	"bytes"
	"fmt"
	"os"
	"sync"
)

// Event 推送给前端的事件（通过 SSE）。
type Event struct {
	Type string // release | log | end
	Data any
}

const maxHistory = 5000

// maxLineLen 单行日志上限：远程输出可能长时间没有换行（如进度条），避免 partial 无限增长
const maxLineLen = 32 << 10

// Hub 单次发布的事件广播中心：保存日志历史，新订阅者先回放再接收实时事件。
type Hub struct {
	mu      sync.Mutex
	logs    []string
	subs    map[chan Event]struct{}
	closed  bool
	logPath string
	logFile *os.File
	logErr  error
	warned  bool // 日志文件不可用的提示是否已输出
	partial bytes.Buffer
}

func newHub(logPath string) *Hub {
	return &Hub{subs: map[chan Event]struct{}{}, logPath: logPath}
}

// openLogLocked 懒打开日志文件：发布日志同时落盘，打开失败需要显式提示而不是静默丢弃
func (h *Hub) openLogLocked() {
	if h.logFile != nil || h.logPath == "" || h.logErr != nil {
		return
	}
	f, err := os.OpenFile(h.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		h.logErr = err
		return
	}
	h.logFile = f
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
		if h.partial.Len() >= maxLineLen {
			h.emitLineLocked(h.partial.String())
			h.partial.Reset()
		}
	}
	return len(p), nil
}

func (h *Hub) Logf(line string) {
	_, _ = h.Write([]byte(line + "\n"))
}

func (h *Hub) emitLineLocked(line string) {
	h.openLogLocked()
	if h.logFile == nil && h.logErr != nil && !h.warned {
		// 只提示一次：日志文件不可用时仍要保证前端 SSE 实时可见
		h.warned = true
		line = fmt.Sprintf("[警告] 发布日志文件 %s 无法写入 (%v)，本次仅保留实时日志", h.logPath, h.logErr)
	}
	if h.logFile != nil {
		if _, err := h.logFile.WriteString(line + "\n"); err != nil && h.logErr == nil {
			h.logErr = err
		}
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
