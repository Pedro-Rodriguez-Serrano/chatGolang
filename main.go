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
	// tamaños (en bytes) de los buffers internos de lectura y escritura
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func serveWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
	//lee el parámetro de la URL y quita espacios. Si no hay le pone anónimo
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		name = "anónimo"
	}
	// si pasa de 20 caracteres se recorta
	// Esta validación se hace en el servidor aunque el HTML
	// ya tenga maxlength="20", porque cualquiera puede saltarse el HTML y conectarse directamente.
	if utf8.RuneCountInString(name) > 20 {
		name = string([]rune(name)[:20])
	}

	/*
		Upgrade toma el control de la conexión de red subyacente y responde al navegador con el código
		101 Switching Protocols. A partir de ahí, conn es un WebSocket por el que se puede leer y escribir
		en ambas direcciones.

		Si falla (por ejemplo, la petición no era un WebSocket válido o el origen no coincide), Upgrade ya
		responde con el error HTTP adecuado, así que aquí solo se registra en el log y se sale. Tras un
		Upgrade correcto no hay que volver a escribir en w.
	*/
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade:", err)
		return
	}

	// Se crea el Client con su nombre, el hub y la conexión, y se envía por el canal register.
	client := NewClient(name, hub, conn)
	hub.register <- client

	//Lanzar las goroutines
	go client.writePump()
	go client.readPump()
}

func main() {
	//Crea el hub y lanza su bucle en una goroutine propia.
	hub := NewHub()
	go hub.Run()

	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/", http.FileServer(http.FS(sub)))

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) { // Asocia la ruta /ws a una función.
		serveWS(hub, w, r)
	})

	// abrir servidor
	addr := ":8080"
	log.Println("Chat en http://localhost" + addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
