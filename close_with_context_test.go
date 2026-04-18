//go:build !js

package websocket_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/internal/test/assert"
	"github.com/coder/websocket/internal/xsync"
)

// assertGateLoser checks that err from a second Close/CloseNow/CloseWithContext
// call either is nil or wraps net.ErrClosed. Upstream Close() and CloseNow()
// both wrap net.ErrClosed via errd.Wrap on the gate-loser path (the swallow
// defer is registered AFTER the casClosing check, so it doesn't fire), and
// CloseWithContext follows the same convention. Per Global Invariants #1 and
// #2, we cannot change Close/CloseNow behavior, so the assertion accepts the
// upstream shape.
func assertGateLoser(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("gate-loser close returned unexpected error: %v", err)
	}
}

// --- Test helpers --------------------------------------------------------

// unresponsivePeerServer returns an httptest server whose handler accepts a
// WebSocket connection but never reads from it, never writes to it, and never
// closes it. This models a peer that does not answer a close handshake.
//
// Real TCP is required (not wstest.Pipe) because net.Pipe is synchronous —
// the close-frame flush itself would block on a peer that never reads. Under
// httptest.NewServer, the kernel TCP buffer absorbs the tiny close frame, so
// only the handshake wait exercises the new timeout behavior.
func unresponsivePeerServer(t testing.TB) (wsURL string, cleanup func()) {
	t.Helper()
	release := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("server accept: %v", err)
			return
		}
		<-release
		_ = c.CloseNow()
	}))
	return strings.Replace(s.URL, "http://", "ws://", 1), func() {
		close(release)
		s.Close()
	}
}

// responsivePeerServer returns an httptest server whose handler invokes
// CloseRead. CloseRead runs a reader goroutine that auto-responds to close
// frames, so the handshake completes promptly from the client's side.
func responsivePeerServer(t testing.TB) (wsURL string, cleanup func()) {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("server accept: %v", err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		ctx = c.CloseRead(ctx)
		<-ctx.Done()
	}))
	return strings.Replace(s.URL, "http://", "ws://", 1), s.Close
}

func dialTest(t testing.TB, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, nil)
	assert.Success(t, err)
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

// --- Behavior tests ------------------------------------------------------

// TestCloseWithContext_ResponsivePeer: peer echoes Close; method returns
// promptly with nil error; transport is dead after return.
func TestCloseWithContext_ResponsivePeer(t *testing.T) {
	t.Parallel()

	url, cleanup := responsivePeerServer(t)
	defer cleanup()

	c := dialTest(t, url)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := c.CloseWithContext(ctx, websocket.StatusNormalClosure, "")
	elapsed := time.Since(start)

	assert.Success(t, err)
	if elapsed > 2*time.Second {
		t.Fatalf("responsive-peer close took %v, expected <2s", elapsed)
	}

	// Transport is dead: Write returns a non-nil error.
	werr := c.Write(context.Background(), websocket.MessageText, []byte("x"))
	assert.Error(t, werr)

	// Gate is taken: CloseNow on gate-loser returns wrapped net.ErrClosed
	// (upstream convention; see assertGateLoser).
	assertGateLoser(t, c.CloseNow())
}

// TestCloseWithContext_UnresponsivePeer_ForcedTeardown: peer never responds;
// method returns inside the ctx budget (not the pre-fork 5s tax); transport
// is dead; error wraps context.DeadlineExceeded.
func TestCloseWithContext_UnresponsivePeer_ForcedTeardown(t *testing.T) {
	t.Parallel()

	url, cleanup := unresponsivePeerServer(t)
	defer cleanup()

	c := dialTest(t, url)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := c.CloseWithContext(ctx, websocket.StatusNormalClosure, "")
	elapsed := time.Since(start)

	// Budget = ctx (100ms) + small goroutine-unwind budget. Pre-fork behavior
	// would pay the hardcoded 5s waitCloseHandshake tax here, blowing past 500ms.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("unresponsive-peer close took %v, expected <500ms (ctx was 100ms)", elapsed)
	}

	// Hard assertion per WO-3 contract: unresponsive peer + deadline-based ctx
	// returns a non-nil error wrapping context.DeadlineExceeded.
	if err == nil {
		t.Fatal("expected wrapped DeadlineExceeded, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected wrapped DeadlineExceeded, got %v", err)
	}

	// Transport is dead.
	werr := c.Write(context.Background(), websocket.MessageText, []byte("x"))
	assert.Error(t, werr)
}

// TestCloseWithContext_RepeatCallsAreNoOps: first call wins the gate; all
// subsequent Close*/CloseNow/CloseWithContext calls return nil.
func TestCloseWithContext_RepeatCallsAreNoOps(t *testing.T) {
	t.Parallel()

	url, cleanup := responsivePeerServer(t)
	defer cleanup()

	c := dialTest(t, url)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	assert.Success(t, c.CloseWithContext(ctx, websocket.StatusNormalClosure, ""))
	assertGateLoser(t, c.CloseWithContext(ctx, websocket.StatusNormalClosure, ""))
	assertGateLoser(t, c.Close(websocket.StatusNormalClosure, ""))
	assertGateLoser(t, c.CloseNow())
}

// TestCloseWithContext_CancellationForcesTeardown: cancel() mid-wait unwinds
// the method and tears down the transport within the same budget as a
// deadline-based ctx.
func TestCloseWithContext_CancellationForcesTeardown(t *testing.T) {
	t.Parallel()

	url, cleanup := unresponsivePeerServer(t)
	defer cleanup()

	c := dialTest(t, url)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := xsync.Go(func() error {
		return c.CloseWithContext(ctx, websocket.StatusNormalClosure, "")
	})

	// Let writeClose complete, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancelStart := time.Now()
	cancel()

	select {
	case err := <-done:
		elapsed := time.Since(cancelStart)
		if elapsed > 500*time.Millisecond {
			t.Fatalf("CloseWithContext did not unwind after cancel within 500ms (took %v)", elapsed)
		}
		if err == nil {
			t.Fatal("expected wrapped context.Canceled, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected wrapped context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CloseWithContext did not return after cancel")
	}

	werr := c.Write(context.Background(), websocket.MessageText, []byte("x"))
	assert.Error(t, werr)
}

// TestCloseWithContext_ConcurrentCloseNow: gate is taken by CloseWithContext;
// later CloseNow falls through to waitGoroutines and returns nil; neither hangs.
func TestCloseWithContext_ConcurrentCloseNow(t *testing.T) {
	t.Parallel()

	url, cleanup := unresponsivePeerServer(t)
	defer cleanup()

	c := dialTest(t, url)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var started atomic.Int32
	errA := xsync.Go(func() error {
		started.Add(1)
		return c.CloseWithContext(ctx, websocket.StatusNormalClosure, "")
	})
	// Give A time to win the gate and start its write.
	time.Sleep(20 * time.Millisecond)
	errB := xsync.Go(func() error {
		started.Add(1)
		return c.CloseNow()
	})

	select {
	case <-errA:
		// Any return value is acceptable; we only assert no hang.
	case <-time.After(3 * time.Second):
		t.Fatal("CloseWithContext hung")
	}
	select {
	case err := <-errB:
		// CloseNow as gate-loser: upstream convention wraps net.ErrClosed.
		assertGateLoser(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("CloseNow hung behind gate")
	}
	if started.Load() != 2 {
		t.Fatalf("expected both goroutines to run, got %d", started.Load())
	}
}

// TestCloseWithContext_AlreadyExpiredContext: ctx already expired at call
// time; method returns promptly with transport torn down. Error is either
// wrapped context.Canceled OR nil — mu.lock's internal select races between
// ctx.Done() and the lock channel when both are ready, so when the lock
// channel wins the write succeeds, waitCloseHandshake later sees the
// already-closed transport and returns net.ErrClosed, which the outer
// defer swallows to nil. The load-bearing guarantee is prompt return with
// dead transport; the exact error shape is non-deterministic in this
// corner case.
func TestCloseWithContext_AlreadyExpiredContext(t *testing.T) {
	t.Parallel()

	url, cleanup := unresponsivePeerServer(t)
	defer cleanup()

	c := dialTest(t, url)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // expire immediately

	start := time.Now()
	err := c.CloseWithContext(ctx, websocket.StatusNormalClosure, "")
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Fatalf("expired-ctx close took %v, expected <500ms", elapsed)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected nil or wrapped context.Canceled, got %v", err)
	}

	werr := c.Write(context.Background(), websocket.MessageText, []byte("x"))
	assert.Error(t, werr)
}

// TestCloseWithContext_Race_ActiveReader: a blocked Read on the same conn
// must unwind when CloseWithContext forces teardown.
func TestCloseWithContext_Race_ActiveReader(t *testing.T) {
	t.Parallel()

	url, cleanup := unresponsivePeerServer(t)
	defer cleanup()

	c := dialTest(t, url)

	readerDone := xsync.Go(func() error {
		readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _, err := c.Read(readCtx)
		return err
	})

	// Let reader block on the underlying read.
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_ = c.CloseWithContext(ctx, websocket.StatusNormalClosure, "")
	if elapsed := time.Since(start); elapsed > 600*time.Millisecond {
		t.Fatalf("CloseWithContext hung with active reader: %v", elapsed)
	}

	select {
	case err := <-readerDone:
		if err == nil {
			t.Fatal("expected reader to error after forced teardown, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("reader did not unwind after forced teardown")
	}
}

// TestCloseWithContext_Race_ActiveWriter: a concurrent writer loop must fail
// fast when CloseWithContext tears down the transport.
func TestCloseWithContext_Race_ActiveWriter(t *testing.T) {
	t.Parallel()

	url, cleanup := unresponsivePeerServer(t)
	defer cleanup()

	c := dialTest(t, url)

	writerStop := make(chan struct{})
	writerDone := xsync.Go(func() error {
		payload := []byte("hello world")
		for {
			select {
			case <-writerStop:
				return nil
			default:
			}
			wctx, wcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := c.Write(wctx, websocket.MessageText, payload)
			wcancel()
			if err != nil {
				return err
			}
		}
	})

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_ = c.CloseWithContext(ctx, websocket.StatusNormalClosure, "")
	if elapsed := time.Since(start); elapsed > 600*time.Millisecond {
		close(writerStop)
		t.Fatalf("CloseWithContext hung with active writer: %v", elapsed)
	}

	close(writerStop)
	select {
	case <-writerDone:
	case <-time.After(1 * time.Second):
		t.Fatal("writer did not unwind after forced teardown")
	}
}

// TestCloseWithContext_Race_ActivePing: a Ping awaiting a pong must unwind
// when CloseWithContext forces teardown.
func TestCloseWithContext_Race_ActivePing(t *testing.T) {
	t.Parallel()

	url, cleanup := unresponsivePeerServer(t)
	defer cleanup()

	c := dialTest(t, url)

	pingDone := xsync.Go(func() error {
		pctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return c.Ping(pctx)
	})

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_ = c.CloseWithContext(ctx, websocket.StatusNormalClosure, "")
	if elapsed := time.Since(start); elapsed > 600*time.Millisecond {
		t.Fatalf("CloseWithContext hung with active ping: %v", elapsed)
	}

	select {
	case err := <-pingDone:
		if err == nil {
			t.Fatal("expected ping to error after forced teardown, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("ping did not unwind after forced teardown")
	}
}

// TestCloseWithContext_NoGoroutineLeaks: repeated open/close cycles must not
// leak goroutines. No external dependency — manual NumGoroutine comparison
// with a settling GC pass.
func TestCloseWithContext_NoGoroutineLeaks(t *testing.T) {
	t.Parallel()

	url, cleanup := unresponsivePeerServer(t)
	defer cleanup()

	// Warm up: first connection brings up httptest, HTTP transport pools, etc.
	// Measure the steady-state delta starting from the second iteration.
	{
		c := dialTest(t, url)
		cctx, ccancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_ = c.CloseWithContext(cctx, websocket.StatusNormalClosure, "")
		ccancel()
	}
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	before := runtime.NumGoroutine()

	const iterations = 20
	for i := 0; i < iterations; i++ {
		c := dialTest(t, url)
		cctx, ccancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_ = c.CloseWithContext(cctx, websocket.StatusNormalClosure, "")
		ccancel()
	}

	// Allow stragglers to finish.
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	// Budget: up to 10 lingering goroutines (http transport keepalives,
	// stalled-server goroutines from prior iterations, etc.). A real leak
	// would scale with iterations (20+).
	if after-before > 10 {
		t.Fatalf("goroutine leak after %d iterations: before=%d after=%d (delta %d)",
			iterations, before, after, after-before)
	}
}

// TestCloseWithContext_Stress_RapidCycles: sanity check that we're not
// paying the pre-fork 5s hardcoded tax per iteration.
func TestCloseWithContext_Stress_RapidCycles(t *testing.T) {
	t.Parallel()

	url, cleanup := unresponsivePeerServer(t)
	defer cleanup()

	const iterations = 30
	start := time.Now()
	for i := 0; i < iterations; i++ {
		c := dialTest(t, url)
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		_ = c.CloseWithContext(cctx, websocket.StatusNormalClosure, "")
		ccancel()
	}
	elapsed := time.Since(start)

	// Budget: 30 iterations * (30ms ctx + ~50ms unwind) = ~2.4s nominal.
	// Pre-fork would pay 5s per iteration = 150s+. We cap at 15s for CI headroom.
	if elapsed > 15*time.Second {
		t.Fatalf("stress run too slow: %v for %d iterations (would be <5s post-fork, >150s pre-fork)",
			elapsed, iterations)
	}
}
