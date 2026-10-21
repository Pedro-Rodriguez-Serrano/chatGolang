package main

import "fmt"

// Hub mantiene los clientes conectados y reparte los mensajes.
// Solo la goroutine de Run toca el mapa, así que no hacen falta mutex.
type Hub struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan Message
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan Message, 64),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.clients[c] = true
			h.send(systemMessage(fmt.Sprintf("%s se unió (%d conectados)", c.Name, len(h.clients))))

		case c := <-h.unregister:
			if h.clients[c] {
				delete(h.clients, c)
				close(c.send)
				h.send(systemMessage(fmt.Sprintf("%s salió (%d conectados)", c.Name, len(h.clients))))
			}

		case msg := <-h.broadcast:
			h.send(msg)
		}
	}
}

// send entrega el mensaje a todos. Si un cliente tiene el buffer lleno
// (es lento o está colgado), se le desconecta para no bloquear a los demás.
func (h *Hub) send(msg Message) {
	for c := range h.clients {
		select {
		case c.send <- msg:
		default:
			delete(h.clients, c)
			close(c.send)
		}
	}
}
