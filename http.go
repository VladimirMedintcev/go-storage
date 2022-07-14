package main

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func helloMuxHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello Gorilla\n"))
}

func main() {
	router := mux.NewRouter()

	router.HandleFunc("/", helloMuxHandler)

	log.Fatal(http.ListenAndServe(":8080", router))
}
