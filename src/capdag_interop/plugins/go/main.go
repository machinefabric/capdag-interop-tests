package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	cborlib "github.com/fxamacker/cbor/v2"

	"github.com/machinefabric/capdag-go/bifaci"
	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// =============================================================================
// Helpers
// =============================================================================

// collectPayload reads all CHUNK frames, decodes each as CBOR, and returns the reconstructed value.
// PROTOCOL: Each CHUNK payload is a complete, independently decodable CBOR value.
func collectPayload(frames <-chan bifaci.Frame) interface{} {
	var chunks []interface{}
	for frame := range frames {
		switch frame.FrameType {
		case bifaci.FrameTypeChunk:
			if frame.Payload != nil {
				var value interface{}
				if err := cborlib.Unmarshal(frame.Payload, &value); err != nil {
					panic(fmt.Sprintf("CHUNK payload must be valid CBOR: %v", err))
				}
				chunks = append(chunks, value)
			}
		case bifaci.FrameTypeEnd:
			goto reconstruct
		}
	}

reconstruct:
	if len(chunks) == 0 {
		return nil
	} else if len(chunks) == 1 {
		return chunks[0]
	}
	switch chunks[0].(type) {
	case []byte:
		var accumulated []byte
		for _, chunk := range chunks {
			if b, ok := chunk.([]byte); ok {
				accumulated = append(accumulated, b...)
			}
		}
		return accumulated
	case string:
		var accumulated string
		for _, chunk := range chunks {
			if s, ok := chunk.(string); ok {
				accumulated += s
			}
		}
		return accumulated
	default:
		return chunks
	}
}

// collectInputBytes collects all input frames from the request and returns raw bytes.
func collectInputBytes(req *bifaci.Request) ([]byte, error) {
	cborValue := collectPayload(req.Frames())
	return cborValueToBytes(cborValue)
}

// collectJSON collects all input frames and parses as a valueRequest.
func collectJSON(req *bifaci.Request) (valueRequest, error) {
	payload := collectPayload(req.Frames())
	return parseValueRequest(payload)
}

// cborValueToBytes converts a CBOR value to bytes.
func cborValueToBytes(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("expected []byte or string, got %T", v)
	}
}

// valueRequest is the JSON structure for number/string payloads.
type valueRequest struct {
	Value json.RawMessage `json:"value"`
}

func parseValueRequest(cborValue interface{}) (valueRequest, error) {
	var req valueRequest
	switch v := cborValue.(type) {
	case map[interface{}]interface{}:
		if val, ok := v["value"]; ok {
			valueBytes, err := json.Marshal(val)
			if err != nil {
				return req, fmt.Errorf("failed to marshal value: %w", err)
			}
			req.Value = json.RawMessage(valueBytes)
			return req, nil
		}
		return req, fmt.Errorf("missing 'value' field in map")
	case []byte:
		if err := json.Unmarshal(v, &req); err != nil {
			return req, fmt.Errorf("invalid JSON: %w", err)
		}
		return req, nil
	case string:
		if err := json.Unmarshal([]byte(v), &req); err != nil {
			return req, fmt.Errorf("invalid JSON: %w", err)
		}
		return req, nil
	default:
		return req, fmt.Errorf("expected map or []byte, got %T", cborValue)
	}
}

// =============================================================================
// Manifest
// =============================================================================

func buildManifest() *bifaci.CapManifest {
	mustBuild := func(b *urn.CapUrnBuilder) *urn.CapUrn {
		u, err := b.Build()
		if err != nil {
			panic(fmt.Sprintf("failed to build cap URN: %v", err))
		}
		return u
	}

	mustFromString := func(s string) *urn.CapUrn {
		u, err := urn.NewCapUrnFromString(s)
		if err != nil {
			panic(fmt.Sprintf("failed to parse cap URN: %v", err))
		}
		return u
	}

	caps := []cap.Cap{
		// CAP_IDENTITY (required) - use from_string to parse the bare "cap:" constant
		*cap.NewCap(
			mustFromString(standard.CapIdentity),
			"Identity",
			"identity",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "echo").
				InSpec("media:").
				OutSpec("media:")),
			"Echo", "echo",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "double").
				InSpec("media:order-value;json;textable;record").
				OutSpec("media:loyalty-points;integer;textable;numeric")),
			"Double", "double",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "stream_chunks").
				InSpec("media:update-count;json;textable;record").
				OutSpec("media:order-updates;textable")),
			"Stream Chunks", "stream_chunks",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "binary_echo").
				InSpec("media:product-image").
				OutSpec("media:product-image")),
			"Binary Echo", "binary_echo",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "slow_response").
				InSpec("media:payment-delay-ms;json;textable;record").
				OutSpec("media:payment-result;textable")),
			"Slow Response", "slow_response",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "generate_large").
				InSpec("media:report-size;json;textable;record").
				OutSpec("media:sales-report")),
			"Generate Large", "generate_large",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "with_status").
				InSpec("media:fulfillment-steps;json;textable;record").
				OutSpec("media:fulfillment-status;textable")),
			"With Status", "with_status",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "throw_error").
				InSpec("media:payment-error;json;textable;record").
				OutSpec("media:void")),
			"Throw Error", "throw_error",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "peer_echo").
				InSpec("media:customer-message;textable").
				OutSpec("media:customer-message;textable")),
			"Peer Echo", "peer_echo",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "nested_call").
				InSpec("media:order-value;json;textable;record").
				OutSpec("media:final-price;integer;textable;numeric")),
			"Nested Call", "nested_call",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "heartbeat_stress").
				InSpec("media:monitoring-duration-ms;json;textable;record").
				OutSpec("media:health-status;textable")),
			"Heartbeat Stress", "heartbeat_stress",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "concurrent_stress").
				InSpec("media:order-batch-size;json;textable;record").
				OutSpec("media:batch-result;textable")),
			"Concurrent Stress", "concurrent_stress",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "get_manifest").
				InSpec("media:void").
				OutSpec("media:service-capabilities;json;textable;record")),
			"Get Manifest", "get_manifest",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "process_large").
				InSpec("media:uploaded-document").
				OutSpec("media:document-info;json;textable;record")),
			"Process Large", "process_large",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "hash_incoming").
				InSpec("media:uploaded-document").
				OutSpec("media:document-hash;textable")),
			"Hash Incoming", "hash_incoming",
		),
		*cap.NewCap(
			mustBuild(urn.NewCapUrnBuilder().
				Tag("op", "verify_binary").
				InSpec("media:package-data").
				OutSpec("media:verification-status;textable")),
			"Verify Binary", "verify_binary",
		),
		func() cap.Cap {
			stdin := "media:"
			position := 0
			argDesc := "Path to invoice file to read"

			c := cap.NewCap(
				mustBuild(urn.NewCapUrnBuilder().
					Tag("op", "read_file_info").
					InSpec("media:invoice;file-path;textable").
					OutSpec("media:invoice-metadata;json;textable;record")),
				"Read File Info", "read_file_info",
			)
			c.Args = []cap.CapArg{
				{
					MediaUrn: "media:invoice;file-path;textable",
					Required: true,
					Sources: []cap.ArgSource{
						{Stdin: &stdin},
						{Position: &position},
					},
					ArgDescription: argDesc,
				},
			}
			c.Output = &cap.CapOutput{
				MediaUrn:          "media:invoice-metadata;json;textable;record",
				OutputDescription: "Invoice file size and SHA256 checksum",
			}
			return *c
		}(),
	}

	return bifaci.NewCapManifest(
		"InteropTestPlugin",
		"1.0.0",
		"Interoperability testing plugin (Go)",
		caps,
	)
}

// =============================================================================
// Op Implementations
// =============================================================================

// === STREAMING OPS (no accumulation) ===

type EchoOp struct{}

func (op *EchoOp) Perform(req *bifaci.Request) error {
	cborValue := collectPayload(req.Frames())
	payloadBytes, err := cborValueToBytes(cborValue)
	if err != nil {
		return err
	}
	return req.Output().EmitCbor(payloadBytes)
}

type BinaryEchoOp struct{}

func (op *BinaryEchoOp) Perform(req *bifaci.Request) error {
	cborValue := collectPayload(req.Frames())
	payloadBytes, err := cborValueToBytes(cborValue)
	if err != nil {
		return err
	}
	return req.Output().EmitCbor(payloadBytes)
}

type PeerEchoOp struct{}

func (op *PeerEchoOp) Perform(req *bifaci.Request) error {
	cborValue := collectPayload(req.Frames())
	payloadBytes, err := cborValueToBytes(cborValue)
	if err != nil {
		return err
	}
	args := []cap.CapArgumentValue{
		cap.NewCapArgumentValue("media:customer-message;textable", payloadBytes),
	}
	peerFrames, err := req.Peer().Invoke("cap:in=media:;out=media:", args)
	if err != nil {
		return fmt.Errorf("peer invoke failed: %w", err)
	}
	output := req.Output()
	var dataValue interface{}
	var hasData bool
	for frame := range peerFrames {
		switch frame.FrameType {
		case bifaci.FrameTypeLog:
			if p, ok := frame.LogProgress(); ok {
				mapped := p * 0.25 // base=0.0, weight=0.25
				msg := frame.LogMessage()
				output.Progress(mapped, msg)
			} else if msg := frame.LogMessage(); msg != "" {
				level := frame.LogLevel()
				if level == "" {
					level = "info"
				}
				output.EmitLog(level, msg)
			}
		case bifaci.FrameTypeChunk:
			if frame.Payload != nil {
				var value interface{}
				if err := cborlib.Unmarshal(frame.Payload, &value); err != nil {
					return fmt.Errorf("invalid CBOR in CHUNK: %w", err)
				}
				dataValue = value
				hasData = true
			}
		case bifaci.FrameTypeEnd:
			break
		case bifaci.FrameTypeErr:
			code := frame.ErrorCode()
			message := frame.ErrorMessage()
			return fmt.Errorf("[%s] %s", code, message)
		}
	}
	if !hasData {
		return fmt.Errorf("no data from peer")
	}
	return output.EmitCbor(dataValue)
}

// === ACCUMULATING OPS ===

type DoubleOp struct{}

func (op *DoubleOp) Perform(req *bifaci.Request) error {
	fmt.Fprintln(os.Stderr, "[double] Handler starting")
	r, err := collectJSON(req)
	if err != nil {
		return err
	}
	var value uint64
	if err := json.Unmarshal(r.Value, &value); err != nil {
		return fmt.Errorf("expected number: %w", err)
	}
	result := value * 2
	fmt.Fprintf(os.Stderr, "[double] Parsed value: %d, doubling to: %d\n", value, result)
	if err := req.Output().EmitCbor(result); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "[double] Handler complete")
	return nil
}

type StreamChunksOp struct{}

func (op *StreamChunksOp) Perform(req *bifaci.Request) error {
	r, err := collectJSON(req)
	if err != nil {
		return err
	}
	var count uint64
	if err := json.Unmarshal(r.Value, &count); err != nil {
		return fmt.Errorf("expected number: %w", err)
	}
	output := req.Output()
	for i := uint64(0); i < count; i++ {
		output.Progress(float32(i)/float32(count), "streaming")
		chunk := fmt.Sprintf("chunk-%d", i)
		if err := output.EmitCbor([]byte(chunk)); err != nil {
			return err
		}
	}
	output.Progress(1.0, "done")
	return output.EmitCbor([]byte("done"))
}

type SlowResponseOp struct{}

func (op *SlowResponseOp) Perform(req *bifaci.Request) error {
	r, err := collectJSON(req)
	if err != nil {
		return err
	}
	var sleepMs uint64
	if err := json.Unmarshal(r.Value, &sleepMs); err != nil {
		return fmt.Errorf("expected number: %w", err)
	}
	output := req.Output()
	var elapsed uint64
	output.Progress(0.0, "waiting")
	for elapsed < sleepMs {
		chunk := sleepMs - elapsed
		if chunk > 100 {
			chunk = 100
		}
		time.Sleep(time.Duration(chunk) * time.Millisecond)
		elapsed += chunk
		output.Progress(float32(elapsed)/float32(sleepMs), "waiting")
	}
	response := fmt.Sprintf("slept-%dms", sleepMs)
	return output.EmitCbor([]byte(response))
}

type GenerateLargeOp struct{}

func (op *GenerateLargeOp) Perform(req *bifaci.Request) error {
	r, err := collectJSON(req)
	if err != nil {
		return err
	}
	var size uint64
	if err := json.Unmarshal(r.Value, &size); err != nil {
		return fmt.Errorf("expected number: %w", err)
	}
	output := req.Output()
	pattern := []byte("ABCDEFGH")
	result := make([]byte, size)
	output.Progress(0.0, "generating")
	for i := uint64(0); i < size; i++ {
		result[i] = pattern[i%uint64(len(pattern))]
		if i%65536 == 0 && size > 0 {
			output.Progress(float32(i)/float32(size), "generating")
		}
	}
	output.Progress(1.0, "generating")
	return output.EmitCbor(result)
}

type WithStatusOp struct{}

func (op *WithStatusOp) Perform(req *bifaci.Request) error {
	r, err := collectJSON(req)
	if err != nil {
		return err
	}
	var steps uint64
	if err := json.Unmarshal(r.Value, &steps); err != nil {
		return fmt.Errorf("expected number: %w", err)
	}
	output := req.Output()
	for i := uint64(0); i < steps; i++ {
		output.Progress(float32(i)/float32(steps), fmt.Sprintf("step %d", i))
		output.EmitLog("processing", fmt.Sprintf("step %d", i))
		time.Sleep(10 * time.Millisecond)
	}
	output.Progress(1.0, "completed")
	return output.EmitCbor([]byte("completed"))
}

type ThrowErrorOp struct{}

func (op *ThrowErrorOp) Perform(req *bifaci.Request) error {
	r, err := collectJSON(req)
	if err != nil {
		return err
	}
	var message string
	if err := json.Unmarshal(r.Value, &message); err != nil {
		return fmt.Errorf("expected string: %w", err)
	}
	return fmt.Errorf("%s", message)
}

type NestedCallOp struct{}

func (op *NestedCallOp) Perform(req *bifaci.Request) error {
	fmt.Fprintln(os.Stderr, "[nested_call] Starting handler")
	r, err := collectJSON(req)
	if err != nil {
		return err
	}
	var value uint64
	if err := json.Unmarshal(r.Value, &value); err != nil {
		return fmt.Errorf("expected number: %w", err)
	}
	fmt.Fprintf(os.Stderr, "[nested_call] Parsed value: %d\n", value)

	input, err := json.Marshal(map[string]uint64{"value": value})
	if err != nil {
		return err
	}
	args := []cap.CapArgumentValue{
		cap.NewCapArgumentValue("media:order-value;json;textable;record", input),
	}

	fmt.Fprintln(os.Stderr, "[nested_call] Calling peer double")
	peerFrames, err := req.Peer().Invoke(
		`cap:in="media:order-value;json;textable;record";op=double;out="media:loyalty-points;integer;textable;numeric"`,
		args,
	)
	if err != nil {
		return fmt.Errorf("peer invoke failed: %w", err)
	}

	output := req.Output()
	var cborValue interface{}
	var hasData bool
	for frame := range peerFrames {
		switch frame.FrameType {
		case bifaci.FrameTypeLog:
			if p, ok := frame.LogProgress(); ok {
				mapped := p * 0.25 // base=0.0, weight=0.25
				msg := frame.LogMessage()
				output.Progress(mapped, msg)
			} else if msg := frame.LogMessage(); msg != "" {
				level := frame.LogLevel()
				if level == "" {
					level = "info"
				}
				output.EmitLog(level, msg)
			}
		case bifaci.FrameTypeChunk:
			if frame.Payload != nil {
				var val interface{}
				if err := cborlib.Unmarshal(frame.Payload, &val); err != nil {
					return fmt.Errorf("invalid CBOR in CHUNK: %w", err)
				}
				cborValue = val
				hasData = true
			}
		case bifaci.FrameTypeEnd:
			break
		case bifaci.FrameTypeErr:
			code := frame.ErrorCode()
			message := frame.ErrorMessage()
			return fmt.Errorf("[%s] %s", code, message)
		}
	}
	if !hasData {
		return fmt.Errorf("no data from peer double")
	}
	fmt.Fprintf(os.Stderr, "[nested_call] Peer response: %v\n", cborValue)

	var hostResult uint64
	switch v := cborValue.(type) {
	case uint64:
		hostResult = v
	case int64:
		hostResult = uint64(v)
	case int:
		hostResult = uint64(v)
	default:
		return fmt.Errorf("expected integer from double, got %T", v)
	}

	finalResult := hostResult * 2
	fmt.Fprintf(os.Stderr, "[nested_call] Final result: %d\n", finalResult)

	finalBytes, err := json.Marshal(finalResult)
	if err != nil {
		return err
	}
	return output.EmitCbor(finalBytes)
}

type HeartbeatStressOp struct{}

func (op *HeartbeatStressOp) Perform(req *bifaci.Request) error {
	r, err := collectJSON(req)
	if err != nil {
		return err
	}
	var durationMs uint64
	if err := json.Unmarshal(r.Value, &durationMs); err != nil {
		return fmt.Errorf("expected number: %w", err)
	}
	output := req.Output()
	chunks := durationMs / 100
	remainder := durationMs % 100
	output.Progress(0.0, "heartbeat")
	for i := uint64(0); i < chunks; i++ {
		time.Sleep(100 * time.Millisecond)
		maxChunks := chunks
		if maxChunks == 0 {
			maxChunks = 1
		}
		output.Progress(float32(i+1)/float32(maxChunks), "heartbeat")
	}
	if remainder > 0 {
		time.Sleep(time.Duration(remainder) * time.Millisecond)
	}
	output.Progress(1.0, "heartbeat")
	response := fmt.Sprintf("stressed-%dms", durationMs)
	return output.EmitCbor([]byte(response))
}

type ConcurrentStressOp struct{}

func (op *ConcurrentStressOp) Perform(req *bifaci.Request) error {
	r, err := collectJSON(req)
	if err != nil {
		return err
	}
	var workUnits uint64
	if err := json.Unmarshal(r.Value, &workUnits); err != nil {
		return fmt.Errorf("expected number: %w", err)
	}
	output := req.Output()
	output.Progress(0.0, "starting")
	var sum uint64
	for i := uint64(0); i < workUnits*1000; i++ {
		sum += i
	}
	output.Progress(1.0, "done")
	response := fmt.Sprintf("computed-%d", sum)
	return output.EmitCbor([]byte(response))
}

type GetManifestOp struct{}

func (op *GetManifestOp) Perform(req *bifaci.Request) error {
	_ = collectPayload(req.Frames()) // consume frames even if empty
	manifest := buildManifest()
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	return req.Output().EmitCbor(manifestBytes)
}

type ProcessLargeOp struct{}

func (op *ProcessLargeOp) Perform(req *bifaci.Request) error {
	payload, err := collectInputBytes(req)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(payload)
	checksum := hex.EncodeToString(hash[:])
	result := map[string]interface{}{
		"size":     len(payload),
		"checksum": checksum,
	}
	resultBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return req.Output().EmitCbor(resultBytes)
}

type HashIncomingOp struct{}

func (op *HashIncomingOp) Perform(req *bifaci.Request) error {
	payload, err := collectInputBytes(req)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(payload)
	hexHash := hex.EncodeToString(hash[:])
	return req.Output().EmitCbor([]byte(hexHash))
}

type VerifyBinaryOp struct{}

func (op *VerifyBinaryOp) Perform(req *bifaci.Request) error {
	payload, err := collectInputBytes(req)
	if err != nil {
		return err
	}
	present := make(map[byte]bool)
	for _, b := range payload {
		present[b] = true
	}
	var message string
	if len(present) != 256 {
		message = fmt.Sprintf("missing %d byte values", 256-len(present))
	} else {
		message = "ok"
	}
	return req.Output().EmitCbor([]byte(message))
}

type ReadFileInfoOp struct{}

func (op *ReadFileInfoOp) Perform(req *bifaci.Request) error {
	payload, err := collectInputBytes(req)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(payload)
	checksum := hex.EncodeToString(hash[:])
	result := map[string]interface{}{
		"size":     len(payload),
		"checksum": checksum,
	}
	resultBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return req.Output().EmitCbor(resultBytes)
}

// =============================================================================
// Main
// =============================================================================

func main() {
	manifest := buildManifest()
	runtime, err := bifaci.NewPluginRuntimeWithManifest(manifest)
	if err != nil {
		panic(fmt.Sprintf("failed to create plugin runtime: %v", err))
	}

	// Register all handlers as CapOp types (mirrors Rust register_op_type::<T>())
	runtime.RegisterOp(`cap:in="media:";op=echo;out="media:"`, &EchoOp{})
	runtime.RegisterOp(`cap:in="media:order-value;json;textable;record";op=double;out="media:loyalty-points;integer;textable;numeric"`, &DoubleOp{})
	runtime.RegisterOp(`cap:in="media:update-count;json;textable;record";op=stream_chunks;out="media:order-updates;textable"`, &StreamChunksOp{})
	runtime.RegisterOp(`cap:in="media:product-image";op=binary_echo;out="media:product-image"`, &BinaryEchoOp{})
	runtime.RegisterOp(`cap:in="media:payment-delay-ms;json;textable;record";op=slow_response;out="media:payment-result;textable"`, &SlowResponseOp{})
	runtime.RegisterOp(`cap:in="media:report-size;json;textable;record";op=generate_large;out="media:sales-report"`, &GenerateLargeOp{})
	runtime.RegisterOp(`cap:in="media:fulfillment-steps;json;textable;record";op=with_status;out="media:fulfillment-status;textable"`, &WithStatusOp{})
	runtime.RegisterOp(`cap:in="media:payment-error;json;textable;record";op=throw_error;out=media:void`, &ThrowErrorOp{})
	runtime.RegisterOp(`cap:in="media:customer-message;textable";op=peer_echo;out="media:customer-message;textable"`, &PeerEchoOp{})
	runtime.RegisterOp(`cap:in="media:order-value;json;textable;record";op=nested_call;out="media:final-price;integer;textable;numeric"`, &NestedCallOp{})
	runtime.RegisterOp(`cap:in="media:monitoring-duration-ms;json;textable;record";op=heartbeat_stress;out="media:health-status;textable"`, &HeartbeatStressOp{})
	runtime.RegisterOp(`cap:in="media:order-batch-size;json;textable;record";op=concurrent_stress;out="media:batch-result;textable"`, &ConcurrentStressOp{})
	runtime.RegisterOp(`cap:in=media:void;op=get_manifest;out="media:service-capabilities;json;textable;record"`, &GetManifestOp{})
	runtime.RegisterOp(`cap:in="media:uploaded-document";op=process_large;out="media:document-info;json;textable;record"`, &ProcessLargeOp{})
	runtime.RegisterOp(`cap:in="media:uploaded-document";op=hash_incoming;out="media:document-hash;textable"`, &HashIncomingOp{})
	runtime.RegisterOp(`cap:in="media:package-data";op=verify_binary;out="media:verification-status;textable"`, &VerifyBinaryOp{})
	runtime.RegisterOp(`cap:in="media:invoice;file-path;textable";op=read_file_info;out="media:invoice-metadata;json;textable;record"`, &ReadFileInfoOp{})

	if err := runtime.Run(); err != nil {
		panic(fmt.Sprintf("plugin runtime error: %v", err))
	}
}
