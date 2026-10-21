package main

import "time"

// Message es lo que viaja por el WebSocket hacia los navegadores.
type Message struct {
	Type string    `json:"type"` // "chat" o "system"
	From string    `json:"from,omitempty"`
	Text string    `json:"text"`
	Time time.Time `json:"time"`
}

func systemMessage(text string) Message {
	return Message{Type: "system", Text: text, Time: time.Now()}
}
