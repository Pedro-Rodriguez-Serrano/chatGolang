package main

import (
	"embed"
	"io/fs"
	"log"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

//go:embed static
var staticFiles embed.FS

// Por defecto gorilla rechaza orígenes distintos al host (protección CSRF).
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func serveWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		name = "anónimo"
	}
	if utf8.RuneCountInString(name) > 20 {
		name = string([]rune(name)[:20])
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade:", err)
		return
	}

	client := NewClient(name, hub, conn)
	hub.register <- client

	go client.writePump()
	go client.readPump()
}

func main() {
	hub := NewHub()
	go hub.Run()

	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Fatal(err)
	}
	http.Handle("/", http.FileServer(http.FS(sub)))
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(hub, w, r)
	})

	addr := ":8080"
	log.Println("Chat en http://localhost" + addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
