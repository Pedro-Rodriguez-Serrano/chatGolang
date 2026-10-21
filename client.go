package main

import (
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = pongWait * 9 / 10
	maxMessageSize = 2048
	maxTextRunes   = 500
)

// Client representa a un usuario conectado.
type Client struct {
	Name string
	hub  *Hub
	conn *websocket.Conn
	send chan Message
}

func NewClient(name string, hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		Name: name,
		hub:  hub,
		conn: conn,
		send: make(chan Message, 32),
	}
}

// incoming es el JSON que envía el navegador.
type incoming struct {
	Text string `json:"text"`
}

// readPump lee del navegador y pasa los mensajes al hub.
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		var in incoming
		if err := c.conn.ReadJSON(&in); err != nil {
			return
		}
		text := strings.TrimSpace(in.Text)
		if text == "" {
			continue
		}
		if utf8.RuneCountInString(text) > maxTextRunes {
			text = string([]rune(text)[:maxTextRunes])
		}
		c.hub.broadcast <- Message{
			Type: "chat",
			From: c.Name,
			Text: text,
			Time: time.Now(),
		}
	}
}

// writePump envía al navegador lo que llega por c.send y manda pings.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// El hub cerró el canal.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteJSON(msg); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
