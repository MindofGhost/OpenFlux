package yandex

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/utils"
)

type YandexDocsInfo struct {
	CookieStr   string
	Token       string
	DocID       string
	CallbackURL string
	UserID      string
	Origin      string
	Host        string
	WsURL       string
	Permissions map[string]interface{}
	OpenCmd     map[string]interface{}
}

type DocSession struct {
	Info       YandexDocsInfo
	Conn       *websocket.Conn
	WriteQueue chan []byte
	UserID     string
	writeMu    sync.Mutex
}

func (s *DocSession) safeWrite(messageType int, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return s.Conn.WriteMessage(messageType, data)
}

type YandexDocsTransport struct {
	*transport.BaseTransport
	url         string
	session     *DocSession
	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
}

func NewYandexDocsTransport(url string, config transport.TransportConfig) *YandexDocsTransport {
	return &YandexDocsTransport{BaseTransport: transport.NewBaseTransport(config), url: url}
}

func (t *YandexDocsTransport) Start() error {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	if t.cancel != nil {
		return fmt.Errorf("transport already started")
	}
	config := t.GetConfig()
	if config.MaxQueueSize < 1 || config.KeepAliveInterval <= 0 {
		return fmt.Errorf("invalid queue size or keep-alive interval")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.cancel, t.done = cancel, make(chan struct{})
	t.BaseTransport.Start()
	go func() {
		defer close(t.done)
		t.run(ctx)
	}()
	return nil
}

func (t *YandexDocsTransport) Stop() error {
	t.lifecycleMu.Lock()
	cancel, done := t.cancel, t.done
	if cancel != nil {
		cancel()
	}
	t.BaseTransport.Stop()
	t.lifecycleMu.Unlock()
	if done != nil {
		<-done
	}
	return nil
}

func (t *YandexDocsTransport) Send(data []byte) error {
	t.Mu.RLock()
	defer t.Mu.RUnlock()
	if !t.IsConnected() || t.session == nil {
		return fmt.Errorf("transport not connected")
	}
	// Own the queued bytes. Never carry a failed session's queue into a new one.
	select {
	case t.session.WriteQueue <- append([]byte(nil), data...):
		t.RecordSend(len(data))
		return nil
	default:
		return fmt.Errorf("write queue full")
	}
}

func (t *YandexDocsTransport) run(ctx context.Context) {
	config := t.GetConfig()
	delay := config.ReconnectDelay
	if delay < time.Second {
		delay = time.Second
	}
	userID := randUserID()
	for attempt := 0; ctx.Err() == nil; attempt++ {
		err := t.connect(ctx, userID)
		if ctx.Err() != nil {
			return
		}
		utils.Debugf("[YDOCS] Session ended: %v", err)
		if attempt >= config.MaxReconnectAttempts {
			return
		}
		t.RecordReconnect()
		utils.Debugf("[YDOCS] Reconnecting in %v", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if config.ReconnectMultiplier > 1 {
			delay = min(time.Duration(float64(delay)*config.ReconnectMultiplier), 30*time.Second)
		}
	}
}

func (t *YandexDocsTransport) connect(ctx context.Context, userID string) error {
	utils.Debugf("[YDOCS] Fetching document config")
	info, err := t.fetchDocInfo(ctx, t.url, userID)
	if err != nil {
		return fmt.Errorf("document config: %w", err)
	}
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	headers := http.Header{}
	headers.Set("User-Agent", "Mozilla/5.0")
	headers.Set("Origin", info.Origin)
	headers.Set("Cookie", info.CookieStr)
	headers.Set("Host", info.Host)
	utils.Debugf("[YDOCS] Connecting WebSocket")
	conn, _, err := dialer.DialContext(ctx, info.WsURL, headers)
	if err != nil {
		return fmt.Errorf("WebSocket handshake: %w", err)
	}
	conn.SetReadLimit(8 << 20)
	session := &DocSession{Info: info, Conn: conn, UserID: userID,
		WriteQueue: make(chan []byte, t.GetConfig().MaxQueueSize)}
	return t.serveSession(ctx, session)
}

func (t *YandexDocsTransport) serveSession(parent context.Context, session *DocSession) error {
	ctx, cancel := context.WithCancel(parent)
	var workers sync.WaitGroup
	defer func() {
		// Publish disconnect before any next session can be established.
		t.Mu.Lock()
		t.SetConnected(false)
		t.session = nil
		t.Mu.Unlock()
		cancel()
		session.Conn.Close()
		workers.Wait()
	}()
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-ctx.Done()
		session.Conn.Close()
	}()
	if err := t.authenticate(session); err != nil {
		return fmt.Errorf("document authentication: %w", err)
	}
	t.Mu.Lock()
	if ctx.Err() != nil {
		t.Mu.Unlock()
		return ctx.Err()
	}
	t.session = session
	t.SetConnected(true)
	t.Mu.Unlock()
	workers.Add(1)
	go func() {
		defer workers.Done()
		t.writerLoop(ctx, session)
	}()
	for {
		// Socket.IO pings normally keep this fresh. A silent socket must not
		// leave the transport appearing connected forever.
		if err := session.Conn.SetReadDeadline(time.Now().Add(max(time.Minute, 3*t.GetConfig().KeepAliveInterval))); err != nil {
			return err
		}
		_, message, err := session.Conn.ReadMessage()
		if err != nil {
			return err
		}
		t.handleMessage(session, message)
	}
}

// Engine.IO open -> Socket.IO connect -> document auth must complete in order.
// IsConnected is only published after ONLYOFFICE confirms result=1.
func (t *YandexDocsTransport) authenticate(session *DocSession) error {
	if err := session.Conn.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		return err
	}
	info := session.Info
	engineOpen, socketOpen := false, false
	for {
		_, data, err := session.Conn.ReadMessage()
		if err != nil {
			return err
		}
		text := string(data)
		switch {
		case strings.HasPrefix(text, "0"):
			if engineOpen {
				return fmt.Errorf("duplicate Engine.IO open")
			}
			engineOpen = true
			utils.Debugf("[YDOCS] Engine.IO opened")
			auth, _ := json.Marshal(map[string]string{"token": info.Token})
			if err := session.safeWrite(websocket.TextMessage, append([]byte("40"), auth...)); err != nil {
				return err
			}
		case strings.HasPrefix(text, "40"):
			if !engineOpen || socketOpen {
				return fmt.Errorf("unexpected Socket.IO connect")
			}
			socketOpen = true
			utils.Debugf("[YDOCS] Socket.IO connected")
			auth := map[string]interface{}{
				"type": "auth", "docid": info.DocID, "token": "fghhfgsjdgfjs",
				"user": map[string]interface{}{"id": session.UserID}, "editorType": 0,
				"lastOtherSaveTime": -1, "permissions": info.Permissions,
				"openCmd": info.OpenCmd, "coEditingMode": "fast", "jwtOpen": info.Token,
			}
			message, _ := json.Marshal([]interface{}{"message", auth})
			if err := session.safeWrite(websocket.TextMessage, append([]byte("42"), message...)); err != nil {
				return err
			}
		case strings.HasPrefix(text, "44"):
			return fmt.Errorf("Socket.IO authentication rejected")
		case text == "1" || strings.HasPrefix(text, "41"):
			return fmt.Errorf("server disconnected during authentication")
		case strings.HasPrefix(text, "42"):
			metadata := eventMetadata(data)
			switch metadata.Type {
			case "auth":
				if !socketOpen || metadata.Result != 1 {
					return fmt.Errorf("document auth result=%d", metadata.Result)
				}
				utils.Debugf("[YDOCS] Document authenticated")
				return nil
			case "authChanges":
				if err := session.safeWrite(websocket.TextMessage, []byte(`42["message",{"type":"authChangesAck"}]`)); err != nil {
					return err
				}
			case "error", "drop", "disconnectReason":
				return fmt.Errorf("document event %s (code=%d)", metadata.Type, metadata.Code)
			default:
				utils.Debugf("[YDOCS] Auth event %s", metadata.Type)
			}
		case text == "2":
			if err := session.safeWrite(websocket.TextMessage, []byte("3")); err != nil {
				return err
			}
		}
	}
}

type docEvent struct {
	Type     string `json:"type"`
	Code     int    `json:"code"`
	Result   int    `json:"result"`
	WaitAuth bool   `json:"waitAuth"`
}

func eventMetadata(data []byte) docEvent {
	var event []json.RawMessage
	var metadata docEvent
	if len(data) > 2 && string(data[:2]) == "42" && json.Unmarshal(data[2:], &event) == nil && len(event) == 2 {
		_ = json.Unmarshal(event[1], &metadata)
	}
	return metadata
}

func (t *YandexDocsTransport) writerLoop(ctx context.Context, session *DocSession) {
	ticker := time.NewTicker(t.GetConfig().KeepAliveInterval)
	defer ticker.Stop()
	for {
		var message string
		select {
		case <-ctx.Done():
			return
		case packet := <-session.WriteQueue:
			message = fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, base64.StdEncoding.EncodeToString(packet))
		case <-ticker.C:
			message = `42["message",{"type":"cursor","cursor":"18;---KA---"}]`
		}
		if ctx.Err() != nil {
			return
		}
		if err := session.safeWrite(websocket.TextMessage, []byte(message)); err != nil {
			utils.Debugf("[YDOCS] Write failed: %v", err)
			session.Conn.Close() // Unblock the reader and reconnect once.
			return
		}
	}
}

func (t *YandexDocsTransport) handleMessage(session *DocSession, data []byte) {
	metadata := eventMetadata(data)
	if metadata.Type != "" && metadata.Type != "cursor" {
		utils.Debugf("[YDOCS] Event %s (code=%d)", metadata.Type, metadata.Code)
	}
	if session != nil && session.Conn != nil {
		switch metadata.Type {
		case "connectState":
			if metadata.WaitAuth {
				// The first editor must acknowledge switching to co-editing.
				// Otherwise ONLYOFFICE times out its auth lock and drops it.
				// We have no document edits to save or content locks to release.
				if err := session.safeWrite(websocket.TextMessage, []byte(`42["message",{"type":"unLockDocument","isSave":false,"unlock":true,"releaseLocks":false}]`)); err != nil {
					session.Conn.Close()
				}
			}
		case "error", "drop", "disconnectReason":
			session.Conn.Close()
			return
		case "authChanges":
			if err := session.safeWrite(websocket.TextMessage, []byte(`42["message",{"type":"authChangesAck"}]`)); err != nil {
				session.Conn.Close()
			}
		}
		if string(data) == "1" || strings.HasPrefix(string(data), "41") {
			session.Conn.Close()
			return
		}
	}
	if string(data) == "2" {
		if session != nil && session.Conn != nil {
			if err := session.safeWrite(websocket.TextMessage, []byte("3")); err != nil {
				session.Conn.Close()
			}
		}
		return
	}
	// A document event can batch several participants' cursors. Decode every
	// payload, including when a keep-alive appears in the same event.
	for _, payload := range extractPayloads(data) {
		decoded, err := base64.StdEncoding.DecodeString(payload)
		if err != nil || len(decoded) == 0 {
			continue
		}
		t.RecordReceive(len(decoded))
		t.CallReceive(decoded)
	}
}

func extractPayloads(data []byte) []string {
	data = []byte(strings.TrimPrefix(string(data), "42"))
	var value interface{}
	if json.Unmarshal(data, &value) != nil {
		return nil
	}
	var payloads []string
	var visit func(interface{})
	visit = func(value interface{}) {
		switch v := value.(type) {
		case []interface{}:
			for _, item := range v {
				visit(item)
			}
		case map[string]interface{}:
			for key, item := range v {
				if str, ok := item.(string); ok {
					switch key {
					case "cursor":
						_, payload, found := strings.Cut(str, ";")
						if found && payload != "---KA---" {
							payloads = append(payloads, payload)
						}
					case "excelAdditionalInfo":
						payloads = append(payloads, str)
					}
				} else {
					visit(item)
				}
			}
		}
	}
	visit(value)
	return payloads
}

func (t *YandexDocsTransport) fetchDocInfo(ctx context.Context, url, userID string) (YandexDocsInfo, error) {
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error { return nil },
		Timeout:       30 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return YandexDocsInfo{}, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := client.Do(req)
	if err != nil {
		return YandexDocsInfo{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return YandexDocsInfo{}, fmt.Errorf("document HTTP status: %s", resp.Status)
	}
	htmlBytes, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return YandexDocsInfo{}, err
	}
	html := string(htmlBytes)

	var cookies []string
	for _, c := range resp.Cookies() {
		cookies = append(cookies, fmt.Sprintf("%s=%s", c.Name, c.Value))
	}

	re := regexp.MustCompile(`(?s)<script[^>]*id="client-config"[^>]*>(.*?)</script>`)
	matches := re.FindStringSubmatch(html)
	if len(matches) < 2 {
		return YandexDocsInfo{}, fmt.Errorf("config not found")
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(matches[1]), &config); err != nil {
		return YandexDocsInfo{}, fmt.Errorf("document config: %w", err)
	}
	officeAction, ok := config["officeActionData"].(map[string]interface{})
	if !ok {
		return YandexDocsInfo{}, fmt.Errorf("officeActionData missing; unsupported editor")
	}

	editorConfigRaw, ok := officeAction["editor_config"].(map[string]interface{})
	if !ok || editorConfigRaw == nil {
		return YandexDocsInfo{}, fmt.Errorf("editor_config nil - will reconnect")
	}

	balancerURL, _ := officeAction["balancer_url"].(string)
	host := strings.TrimPrefix(balancerURL, "https://")
	document, _ := editorConfigRaw["document"].(map[string]interface{})
	token, _ := editorConfigRaw["token"].(string)
	docID, _ := document["key"].(string)
	if !strings.HasPrefix(balancerURL, "https://") || token == "" || docID == "" {
		return YandexDocsInfo{}, fmt.Errorf("incomplete document config; unsupported editor")
	}

	perms, _ := document["permissions"].(map[string]interface{})
	if perms == nil {
		perms = make(map[string]interface{})
	}

	return YandexDocsInfo{
		CookieStr:   strings.Join(cookies, "; "),
		Token:       token,
		DocID:       docID,
		Origin:      balancerURL,
		Host:        host,
		WsURL:       fmt.Sprintf("wss://%s/2024.1.1-375/doc/%s/c/?EIO=4&transport=websocket", host, docID),
		Permissions: perms,
		OpenCmd: map[string]interface{}{
			"c":      "open",
			"id":     docID,
			"userid": userID,
			"format": document["fileType"],
			"url":    document["url"],
			"title":  document["title"],
			"lcid":   25,
		},
	}, nil
}

func randUserID() string {
	return fmt.Sprintf("%010d", rand.New(rand.NewSource(time.Now().UnixNano())).Intn(1000000000))
}
