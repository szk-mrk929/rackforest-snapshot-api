// Package app is the process boundary of the snapshot service.
//
// It serves HTTP in front of the worker. SIGINT and SIGTERM cancel the
// context passed to Run. Run then drains on Config.ShutdownTimeout, which is
// a fresh budget: the signal context is already canceled and must not be the
// deadline of the storage call still running.
//
// Draining rejects every route except GET /healthz with 503. The worker
// stops admitting jobs at the same time, so a snapshot still queued stays
// pending. A storage call already in flight is allowed to finish until the
// budget runs out. If it does not, the call is canceled and the snapshot
// stays creating or deleting.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"rackforest-snapshot-api/src/config"
	"rackforest-snapshot-api/src/domain"
	"rackforest-snapshot-api/src/worker"
)

const readHeaderTimeout = 5 * time.Second

// App is one process: the HTTP server and the worker it drains with.
type App struct {
	cfg    config.Config
	worker *worker.Worker
	log    *slog.Logger

	mux      *http.ServeMux
	srv      *http.Server
	addr     atomic.Value // string, host:port actually bound
	draining atomic.Bool

	shutdownOnce sync.Once
	shutdownErr  error
}

// New starts the worker and builds the server. Routes other than GET /healthz
// are registered on Mux before Run. A nil worker or an invalid config is refused.
func New(cfg config.Config, log *slog.Logger, jobs *worker.Worker) (*App, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if jobs == nil {
		return nil, fmt.Errorf("%w: worker is required", domain.ErrInvalidArgument)
	}
	if log == nil {
		log = slog.Default()
	}
	application := &App{
		cfg:    cfg,
		worker: jobs,
		log:    log,
		mux:    http.NewServeMux(),
	}
	application.srv = &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           application,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	jobs.Start()
	return application, nil
}

// Mux is the router for every route except GET /healthz.
// Register handlers before Run. While the process is draining, ServeHTTP
// answers those routes with 503 and does not call them.
func (a *App) Mux() *http.ServeMux {
	return a.mux
}

// Addr is the host:port Run bound, or "" before Listen succeeds.
func (a *App) Addr() string {
	addr, _ := a.addr.Load().(string)
	return addr
}

// ServeHTTP always serves GET /healthz with 200. Every other request is 503
// once Shutdown has started, including requests that arrived before the
// listener closed.
func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
		writeJSON(w, http.StatusOK, healthBody{Status: "ok"})
		return
	}
	if a.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, apiError{
			Error: apiErrorBody{
				Code:    "shutting_down",
				Message: domain.ErrShuttingDown.Error(),
			},
		})
		return
	}
	a.mux.ServeHTTP(w, r)
}

// Run listens until ctx is canceled or the server stops. The drain that
// follows uses ShutdownTimeout and does not inherit the canceled signal.
func (a *App) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return a.shutdownWithBudget()
	}

	ln, err := net.Listen("tcp", a.cfg.HTTPAddress)
	if err != nil {
		_ = a.shutdownWithBudget()
		return err
	}
	addr := ln.Addr().String()
	a.srv.Addr = addr
	a.addr.Store(addr)

	serveErr := make(chan error, 1)
	go func() {
		err := a.srv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	}()
	a.log.WithGroup("main").Info("http server started", "address", addr)

	select {
	case <-ctx.Done():
		a.log.WithGroup("main").Info("shutdown signal received")
		return a.shutdownWithBudget()
	case err := <-serveErr:
		if err != nil {
			a.log.WithGroup("main").Error("http server stopped", "error", err)
		}
		_ = a.shutdownWithBudget()
		return err
	}
}

func (a *App) shutdownWithBudget() error {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()
	return a.Shutdown(ctx)
}

// Shutdown rejects new HTTP work and new jobs, waits for the storage call
// already running, then closes the listener. A later call returns the same
// error. Queued snapshots are left pending by the worker.
func (a *App) Shutdown(ctx context.Context) error {
	a.shutdownOnce.Do(func() {
		a.shutdownErr = a.shutdown(ctx)
	})
	return a.shutdownErr
}

func (a *App) shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// The listener stays open while the worker waits, so a probe can still
	// read /healthz and so business routes can answer 503 instead of reset.
	a.draining.Store(true)
	a.log.WithGroup("main").Info("shutting down")

	werr := a.worker.Shutdown(ctx)
	if werr != nil {
		a.log.WithGroup("main").Warn("in-flight storage call exceeded the shutdown timeout", "error", werr)
	}
	herr := a.srv.Shutdown(ctx)
	if herr != nil {
		_ = a.srv.Close()
	}
	return errors.Join(werr, herr)
}

type healthBody struct {
	Status string `json:"status"`
}

type apiError struct {
	Error apiErrorBody `json:"error"`
}

type apiErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
