//go:build !js

package websocket

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/coder/websocket/internal/errd"
)

// StatusCode represents a WebSocket status code.
// https://tools.ietf.org/html/rfc6455#section-7.4
type StatusCode int

// https://www.iana.org/assignments/websocket/websocket.xhtml#close-code-number
//
// These are only the status codes defined by the protocol.
//
// You can define custom codes in the 3000-4999 range.
// The 3000-3999 range is reserved for use by libraries, frameworks and applications.
// The 4000-4999 range is reserved for private use.
const (
	StatusNormalClosure   StatusCode = 1000
	StatusGoingAway       StatusCode = 1001
	StatusProtocolError   StatusCode = 1002
	StatusUnsupportedData StatusCode = 1003

	// 1004 is reserved and so unexported.
	statusReserved StatusCode = 1004

	// StatusNoStatusRcvd cannot be sent in a close message.
	// It is reserved for when a close message is received without
	// a status code.
	StatusNoStatusRcvd StatusCode = 1005

	// StatusAbnormalClosure is exported for use only with Wasm.
	// In non Wasm Go, the returned error will indicate whether the
	// connection was closed abnormally.
	StatusAbnormalClosure StatusCode = 1006

	StatusInvalidFramePayloadData StatusCode = 1007
	StatusPolicyViolation         StatusCode = 1008
	StatusMessageTooBig           StatusCode = 1009
	StatusMandatoryExtension      StatusCode = 1010
	StatusInternalError           StatusCode = 1011
	StatusServiceRestart          StatusCode = 1012
	StatusTryAgainLater           StatusCode = 1013
	StatusBadGateway              StatusCode = 1014

	// StatusTLSHandshake is only exported for use with Wasm.
	// In non Wasm Go, the returned error will indicate whether there was
	// a TLS handshake failure.
	StatusTLSHandshake StatusCode = 1015
)

// CloseError is returned when the connection is closed with a status and reason.
//
// Use Go 1.13's errors.As to check for this error.
// Also see the CloseStatus helper.
type CloseError struct {
	Code   StatusCode
	Reason string
}

func (ce CloseError) Error() string {
	return fmt.Sprintf("status = %v and reason = %q", ce.Code, ce.Reason)
}

// CloseStatus is a convenience wrapper around Go 1.13's errors.As to grab
// the status code from a CloseError.
//
// -1 will be returned if the passed error is nil or not a CloseError.
func CloseStatus(err error) StatusCode {
	var ce CloseError
	if errors.As(err, &ce) {
		return ce.Code
	}
	return -1
}

// Close performs the WebSocket close handshake with the given status code and reason.
//
// It will write a WebSocket close frame with a timeout of 5s and then wait 5s for
// the peer to send a close frame.
// All data messages received from the peer during the close handshake will be discarded.
//
// The connection can only be closed once. Additional calls to Close
// are no-ops.
//
// The maximum length of reason must be 125 bytes. Avoid sending a dynamic reason.
//
// Close will unblock all goroutines interacting with the connection once
// complete.
func (c *Conn) Close(code StatusCode, reason string) (err error) {
	defer errd.Wrap(&err, "failed to close WebSocket")

	if c.casClosing() {
		err = c.waitGoroutines()
		if err != nil {
			return err
		}
		return net.ErrClosed
	}
	defer func() {
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err = c.closeHandshake(ctx, code, reason)

	err2 := c.close()
	if err == nil && err2 != nil {
		err = err2
	}

	err2 = c.waitGoroutines()
	if err == nil && err2 != nil {
		err = err2
	}

	return err
}

// CloseWithContext performs the WebSocket close handshake under caller-controlled
// cancellation. It writes a Close frame, then waits for the peer's Close frame
// until ctx is done. If ctx expires or is cancelled before the peer responds,
// the underlying transport is forcibly torn down and CloseWithContext returns.
//
// Unlike Close, the caller owns the timeout. This method guarantees that the
// underlying transport is unusable by the time it returns, even if the peer
// never responds. Existing Close and CloseNow semantics are unchanged.
//
// The connection can only be closed once. If Close, CloseNow, or CloseWithContext
// has already been called, this returns after existing close goroutines have
// unwound.
//
// The maximum length of reason must be 125 bytes. Avoid sending a dynamic reason.
//
// CloseWithContext will unblock all goroutines interacting with the connection
// once complete.
func (c *Conn) CloseWithContext(ctx context.Context, code StatusCode, reason string) (err error) {
	defer errd.Wrap(&err, "failed to close WebSocket")

	if c.casClosing() {
		err = c.waitGoroutines()
		if err != nil {
			return err
		}
		return net.ErrClosed
	}
	defer func() {
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
	}()

	// Best-effort close frame write under caller's ctx. If this fails we still
	// proceed to forced teardown so callers get the transport-death guarantee.
	writeErr := c.writeCloseCtx(ctx, code, reason)

	// Wait for peer close under caller's ctx. On ctx expiry this returns
	// promptly with a ctx error and we proceed to forced teardown.
	waitErr := c.waitCloseHandshake(ctx)

	// Forced transport teardown. c.close() is idempotent (guarded by isClosed),
	// so this is safe even on the happy path where the handshake completed.
	closeErr := c.close()

	// Final goroutine-unwinding safety net (15s hardcoded upstream; unchanged).
	gErr := c.waitGoroutines()

	// Error priority:
	//   1. write error (most actionable — tells caller the frame didn't go out)
	//   2. wait error, if it isn't the expected peer Close with matching code
	//      (ctx.DeadlineExceeded and ctx.Canceled surface here in the forced
	//      teardown path)
	//   3. close error
	//   4. goroutine-unwind error
	switch {
	case writeErr != nil && !errors.Is(writeErr, net.ErrClosed):
		return writeErr
	case waitErr != nil && CloseStatus(waitErr) != code:
		return waitErr
	case closeErr != nil:
		return closeErr
	default:
		return gErr
	}
}

// CloseNow closes the WebSocket connection without attempting a close handshake.
// Use when you do not want the overhead of the close handshake.
func (c *Conn) CloseNow() (err error) {
	defer errd.Wrap(&err, "failed to immediately close WebSocket")

	if c.casClosing() {
		err = c.waitGoroutines()
		if err != nil {
			return err
		}
		return net.ErrClosed
	}
	defer func() {
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
	}()

	err = c.close()

	err2 := c.waitGoroutines()
	if err == nil && err2 != nil {
		err = err2
	}
	return err
}

func (c *Conn) closeHandshake(ctx context.Context, code StatusCode, reason string) error {
	err := c.writeCloseCtx(ctx, code, reason)
	if err != nil {
		return err
	}

	err = c.waitCloseHandshake(ctx)
	if CloseStatus(err) != code {
		return err
	}
	return nil
}

// writeClose preserves the original signature used by non-handshake callers
// (writeError in write.go, peer-initiated close echo in read.go). It wraps
// writeCloseCtx with the historical 5-second default timeout.
func (c *Conn) writeClose(code StatusCode, reason string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	return c.writeCloseCtx(ctx, code, reason)
}

func (c *Conn) writeCloseCtx(ctx context.Context, code StatusCode, reason string) error {
	ce := CloseError{
		Code:   code,
		Reason: reason,
	}

	var p []byte
	var err error
	if ce.Code != StatusNoStatusRcvd {
		p, err = ce.bytes()
		if err != nil {
			return err
		}
	}

	err = c.writeControl(ctx, opClose, p)
	// If the connection closed as we're writing we ignore the error as we might
	// have written the close frame, the peer responded and then someone else read it
	// and closed the connection.
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

func (c *Conn) waitCloseHandshake(ctx context.Context) error {
	err := c.readMu.lock(ctx)
	if err != nil {
		return err
	}
	defer c.readMu.unlock()

	for i := int64(0); i < c.msgReader.payloadLength; i++ {
		_, err := c.br.ReadByte()
		if err != nil {
			return err
		}
	}

	for {
		h, err := c.readLoop(ctx)
		if err != nil {
			return err
		}

		for i := int64(0); i < h.payloadLength; i++ {
			_, err := c.br.ReadByte()
			if err != nil {
				return err
			}
		}
	}
}

func (c *Conn) waitGoroutines() error {
	t := time.NewTimer(time.Second * 15)
	defer t.Stop()

	c.closeReadMu.Lock()
	closeRead := c.closeReadCtx != nil
	c.closeReadMu.Unlock()
	if closeRead {
		select {
		case <-c.closeReadDone:
		case <-t.C:
			return errors.New("failed to wait for close read goroutine to exit")
		}
	}

	select {
	case <-c.closed:
	case <-t.C:
		return errors.New("failed to wait for connection to be closed")
	}

	return nil
}

func parseClosePayload(p []byte) (CloseError, error) {
	if len(p) == 0 {
		return CloseError{
			Code: StatusNoStatusRcvd,
		}, nil
	}

	if len(p) < 2 {
		return CloseError{}, fmt.Errorf("close payload %q too small, cannot even contain the 2 byte status code", p)
	}

	ce := CloseError{
		Code:   StatusCode(binary.BigEndian.Uint16(p)),
		Reason: string(p[2:]),
	}

	if !validWireCloseCode(ce.Code) {
		return CloseError{}, fmt.Errorf("invalid status code %v", ce.Code)
	}

	return ce, nil
}

// See http://www.iana.org/assignments/websocket/websocket.xhtml#close-code-number
// and https://tools.ietf.org/html/rfc6455#section-7.4.1
func validWireCloseCode(code StatusCode) bool {
	switch code {
	case statusReserved, StatusNoStatusRcvd, StatusAbnormalClosure, StatusTLSHandshake:
		return false
	}

	if code >= StatusNormalClosure && code <= StatusBadGateway {
		return true
	}
	if code >= 3000 && code <= 4999 {
		return true
	}

	return false
}

func (ce CloseError) bytes() ([]byte, error) {
	p, err := ce.bytesErr()
	if err != nil {
		err = fmt.Errorf("failed to marshal close frame: %w", err)
		ce = CloseError{
			Code: StatusInternalError,
		}
		p, _ = ce.bytesErr()
	}
	return p, err
}

const maxCloseReason = maxControlPayload - 2

func (ce CloseError) bytesErr() ([]byte, error) {
	if len(ce.Reason) > maxCloseReason {
		return nil, fmt.Errorf("reason string max is %v but got %q with length %v", maxCloseReason, ce.Reason, len(ce.Reason))
	}

	if !validWireCloseCode(ce.Code) {
		return nil, fmt.Errorf("status code %v cannot be set", ce.Code)
	}

	buf := make([]byte, 2+len(ce.Reason))
	binary.BigEndian.PutUint16(buf, uint16(ce.Code))
	copy(buf[2:], ce.Reason)
	return buf, nil
}

func (c *Conn) casClosing() bool {
	return c.closing.Swap(true)
}

func (c *Conn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
