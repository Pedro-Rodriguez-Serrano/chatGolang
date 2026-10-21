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

// Crear nuevo cliente
func NewClient(name string, hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		Name: name,
		hub:  hub,
		conn: conn,
		send: make(chan Message, 32),
	}
}

// Es el JSON que envía el navegador.
type incoming struct {
	Text string `json:"text"`
}

//TODO:

// goroutine que escucha a un cliente. Se queda en un bucle leyendo los mensajes que llegan por
// su WebSocket, los limpia y se los pasa al Hub para que los reparta.
// Cuando la conexión se rompe o el cliente se va, el bucle termina y la función
// se encarga de desregistrar al cliente y cerrar la conexión.
func (cliente *Client) readPump() {
	//defer programa código para ejecutarse cuando la función termine, sea por el motivo que sea.
	defer func() {
		//Avisa al hub de que este cliente se va (unregister)
		cliente.hub.unregister <- cliente
		cliente.conn.Close()
	}()

	//Configuración de la conexión

	//Limita el tamaño de cada mensaje entrante (2048 bytes).
	//Si alguien envía algo mayor, la lectura falla y se cierra la conexión.
	cliente.conn.SetReadLimit(maxMessageSize)

	//sistema para detectar conexiones muertas:

	//“si en 60 segundos (pongWait) no llega nada, da error de lectura”.
	cliente.conn.SetReadDeadline(time.Now().Add(pongWait))
	//se ejecuta cada vez que llega un pong del navegador. Un pong es la respuesta
	// automática del navegador a los pings que manda writePump cada 54 segundos.
	// Cada pong renueva el plazo otros 60 segundos.
	// Si el navegador se cuelga o se corta internet sin cerrar la conexión limpiamente,
	// no llegan pongs, vence el plazo, ReadJSON falla y el cliente se desregistra.
	// Sin esto, quedarían clientes fantasma para siempre.
	cliente.conn.SetPongHandler(func(string) error {
		cliente.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	//bucle principal
	for {
		var in incoming

		//bloquea hasta que llega un mensaje, y lo convierte del JSON
		//Si devuelve error (conexión cerrada, plazo vencido, mensaje demasiado grande o JSON inválido),
		// hace return, lo que dispara el defer.
		if err := cliente.conn.ReadJSON(&in); err != nil {
			return
		}
		//Validación del texto
		text := strings.TrimSpace(in.Text)
		if text == "" {
			continue
		}
		if utf8.RuneCountInString(text) > maxTextRunes {
			text = string([]rune(text)[:maxTextRunes])
		}

		//Crea el Message completo, con el nombre que guarda el servidor (no el que diga el navegador
		// en el mensaje, así nadie puede hacerse pasar por otro) y la hora del servidor. Lo mete en
		// el canal broadcast, y la goroutine del hub lo reparte a todos los clientes.
		cliente.hub.broadcast <- Message{
			Type: "chat",
			From: cliente.Name,
			Text: text,
			Time: time.Now(),
		}
	}
}

// writePump envía al navegador lo que llega y manda pings.
func (cliente *Client) writePump() {

	//crea un reloj que deja un valor en el canal ticker.C cada pingPeriod (54 segundos)
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		cliente.conn.Close()
	}()

	for {
		//select espera a que ocurra cualquiera de los dos eventos y ejecuta el caso que se active primero.
		// Así la misma goroutine atiende mensajes y pings sin necesitar mutex ni dos bucles.
		select {

		//Se saca un mensaje del canal. ok indica si el canal sigue abierto. Antes de
		// escribir se fija un plazo de 10 segundos (writeWait): si la red está bloqueada
		// y la escritura no termina a tiempo, da error en lugar de quedarse colgada para siempre.
		case msg, ok := <-cliente.send:
			cliente.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Si ok es false, el hub cerró el canal (porque el cliente se desregistró,
				// o porque era demasiado lento y lo expulsó). Entonces se envía un mensaje
				// de cierre al navegador, para que se despida de forma limpia, y se termina.
				cliente.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			// Convierte el Message a JSON y lo envía. Si falla (el cliente se fue, la red
			// se cortó), sale y el defer cierra la conexión.
			if err := cliente.conn.WriteJSON(msg); err != nil {
				return
			}

		// Cada 54 segundos se envía un ping. El navegador responde con un pong automáticamente,
		// y ese pong llega a readPump, que con su PongHandler
		// renueva el plazo de 60 segundos. Si el cliente no responde, readPump falla por timeout y se desregistra. Por eso pingPeriod (54 s) es menor que pongWait (60 s): el ping tiene que salir antes de que venza el plazo.
		case <-ticker.C:
			cliente.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := cliente.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
