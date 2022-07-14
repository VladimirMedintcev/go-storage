package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/mux"
)

var logger TransactionLogger

var store = struct {
	sync.RWMutex
	data map[string]string
}{data: make(map[string]string)}

var ErrorNoSuchKey = errors.New("No such key")

func main() {
	initializeTransactionLog()

	router := mux.NewRouter()

	router.HandleFunc("/v1/key/{key}", keyValuePutHandler).Methods("PUT")
	router.HandleFunc("/v1/key/{key}", keyValueGetHandler).Methods("GET")
	router.HandleFunc("/v1/key/{key}", keyValueDeleteHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", router))
}

/**
 * Http handlers.
 */
func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logger.WritePut(key, string(value))

	w.WriteHeader(http.StatusCreated)
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	_, notFoundErr := Get(key)
	if errors.Is(notFoundErr, ErrorNoSuchKey) {
		http.Error(w, notFoundErr.Error(), http.StatusNotFound)
		return
	}

	err := Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logger.WriteDelete(key)

	w.WriteHeader(http.StatusOK)
}

/**
 * Storage functions.
 */
func Get(key string) (string, error) {
	store.RLock()
	value, ok := store.data[key]
	store.RUnlock()

	if !ok {
		return "", ErrorNoSuchKey
	}

	return value, nil
}

func Put(key string, value string) error {
	store.Lock()
	store.data[key] = value
	store.Unlock()

	return nil
}

func Delete(key string) error {
	store.Lock()
	delete(store.data, key)
	store.Unlock()

	return nil
}

/**
 * Transaction logger
 */
type EventType byte

const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

type TransactionLogger interface {
	WritePut(key, value string)
	WriteDelete(key string)
	Err() <-chan error
	ReadEvents() (<-chan Event, <-chan error)
	Run()
}

func initializeTransactionLog() error {
	var err error

	logger, err = NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()
	e, ok := Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors: // Получает ошибки
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete: // Получено событие DELETE!
				err = Delete(e.Key)
			case EventPut: // Получено событие PUT!
				err = Put(e.Key, e.Value)
			}
		}
	}

	logger.Run()

	return err
}

/**
 * File Transaction logger
 */
type FileTransactionLogger struct {
	events       chan<- Event // Канал только для записи; для передачи событий
	errors       <-chan error // Канал только для чтения; для приема ошибок
	lastSequence uint64       // Последний использованный порядковый номер
	file         *os.File     // Местоположение файла журнала
}

func (l *FileTransactionLogger) Run() {
	events := make(chan Event, 16) // Создать канал событий
	l.events = events

	errors := make(chan error, 1) // Создать канал ошибок
	l.errors = errors

	go func() {
		for e := range events { // Извлечь следующее событие Event

			l.lastSequence++ // Увеличить порядковый номер

			_, err := fmt.Fprintf( // Записать событие в журнал
				l.file,
				"%d\t%d\t%s\t%s\n",
				l.lastSequence, e.EventType, e.Key, e.Value)

			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file) // Создать Scanner для чтения l.file
	outEvent := make(chan Event)        // Небуферизованный канал событий
	outError := make(chan error, 1)     // Буферизованный канал ошибок

	go func() {
		var e Event

		defer close(outEvent) // Закрыть каналы
		defer close(outError) // по завершении сопрограммы

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s", &e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}

			// Проверка целостности!
			// Порядковые номера последовательно увеличиваются?
			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}

			l.lastSequence = e.Sequence // Запомнить последний использованный порядковый номер
			outEvent <- e               // Отправить событие along
		}

		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()

	return outEvent, outError
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *FileTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

func NewFileTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)

	if err != nil {
		return nil, fmt.Errorf("Cannot open transaction log file: %w, err")
	}

	return &FileTransactionLogger{file: file}, nil
}
