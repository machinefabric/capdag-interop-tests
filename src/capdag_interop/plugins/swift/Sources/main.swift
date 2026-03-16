import Foundation
import SwiftCBOR
import Bifaci
import CryptoKit
import Ops

// Type aliases to avoid ambiguity with Foundation.OutputStream
typealias BifaciOutputStream = Bifaci.OutputStream
typealias BifaciInputStream = Bifaci.InputStream

// MARK: - Manifest Building

func buildManifest() -> [String: Any] {
    // E-commerce semantic media URNs - must match across all plugin languages
    let caps: [[String: Any]] = [
        [
            "urn": "cap:",
            "title": "Identity",
            "command": "identity"
        ],
        [
            "urn": "cap:in=\"media:\";op=echo;out=\"media:\"",
            "title": "Echo",
            "command": "echo"
        ],
        [
            "urn": "cap:in=\"media:order-value;json;textable;record\";op=double;out=\"media:loyalty-points;integer;textable;numeric\"",
            "title": "Double",
            "command": "double"
        ],
        [
            "urn": "cap:in=\"media:update-count;json;textable;record\";op=stream_chunks;out=\"media:order-updates;textable\"",
            "title": "Stream Chunks",
            "command": "stream_chunks"
        ],
        [
            "urn": "cap:in=\"media:product-image\";op=binary_echo;out=\"media:product-image\"",
            "title": "Binary Echo",
            "command": "binary_echo"
        ],
        [
            "urn": "cap:in=\"media:payment-delay-ms;json;textable;record\";op=slow_response;out=\"media:payment-result;textable\"",
            "title": "Slow Response",
            "command": "slow_response"
        ],
        [
            "urn": "cap:in=\"media:report-size;json;textable;record\";op=generate_large;out=\"media:sales-report\"",
            "title": "Generate Large",
            "command": "generate_large"
        ],
        [
            "urn": "cap:in=\"media:fulfillment-steps;json;textable;record\";op=with_status;out=\"media:fulfillment-status;textable\"",
            "title": "With Status",
            "command": "with_status"
        ],
        [
            "urn": "cap:in=\"media:payment-error;json;textable;record\";op=throw_error;out=\"media:void\"",
            "title": "Throw Error",
            "command": "throw_error"
        ],
        [
            "urn": "cap:in=\"media:customer-message;textable\";op=peer_echo;out=\"media:customer-message;textable\"",
            "title": "Peer Echo",
            "command": "peer_echo"
        ],
        [
            "urn": "cap:in=\"media:order-value;json;textable;record\";op=nested_call;out=\"media:final-price;integer;textable;numeric\"",
            "title": "Nested Call",
            "command": "nested_call"
        ],
        [
            "urn": "cap:in=\"media:monitoring-duration-ms;json;textable;record\";op=heartbeat_stress;out=\"media:health-status;textable\"",
            "title": "Heartbeat Stress",
            "command": "heartbeat_stress"
        ],
        [
            "urn": "cap:in=\"media:order-batch-size;json;textable;record\";op=concurrent_stress;out=\"media:batch-result;textable\"",
            "title": "Concurrent Stress",
            "command": "concurrent_stress"
        ],
        [
            "urn": "cap:in=\"media:void\";op=get_manifest;out=\"media:service-capabilities;json;textable;record\"",
            "title": "Get Manifest",
            "command": "get_manifest"
        ],
        [
            "urn": "cap:in=\"media:uploaded-document\";op=process_large;out=\"media:document-info;json;textable;record\"",
            "title": "Process Large",
            "command": "process_large"
        ],
        [
            "urn": "cap:in=\"media:uploaded-document\";op=hash_incoming;out=\"media:document-hash;textable\"",
            "title": "Hash Incoming",
            "command": "hash_incoming"
        ],
        [
            "urn": "cap:in=\"media:package-data\";op=verify_binary;out=\"media:verification-status;textable\"",
            "title": "Verify Binary",
            "command": "verify_binary"
        ],
        [
            "urn": "cap:in=\"media:invoice;file-path;textable\";op=read_file_info;out=\"media:invoice-metadata;json;textable;record\"",
            "title": "Read File Info",
            "command": "read_file_info",
            "args": [
                [
                    "media_urn": "media:invoice;file-path;textable",
                    "required": true,
                    "sources": [
                        ["stdin": "media:"],
                        ["position": 0]
                    ],
                    "arg_description": "Path to invoice file to read"
                ] as [String: Any]
            ],
            "output": [
                "media_urn": "media:invoice-metadata;json;textable;record",
                "output_description": "Invoice file size and SHA256 checksum"
            ] as [String: Any]
        ]
    ]

    return [
        "name": "InteropTestPlugin",
        "version": "1.0.0",
        "description": "Interoperability testing plugin (Swift)",
        "caps": caps
    ]
}

func buildManifestJSON() -> String {
    let manifest = buildManifest()
    let data = try! JSONSerialization.data(withJSONObject: manifest, options: [.sortedKeys])
    return String(data: data, encoding: .utf8)!
}

// MARK: - Helper Functions

/// Extract first CBOR value from input stream (for single-arg handlers)
func firstValue(from input: InputPackage) throws -> CBOR {
    guard let streamResult = input.nextStream() else {
        throw PluginRuntimeError.handlerError("No input stream")
    }
    let stream = try streamResult.get()
    return try stream.collectValue()
}

/// Convert CBOR value to Data
func cborToData(_ value: CBOR) throws -> Data {
    switch value {
    case .byteString(let bytes):
        return Data(bytes)
    case .utf8String(let text):
        return text.data(using: .utf8)!
    default:
        throw PluginRuntimeError.handlerError("Expected byteString or utf8String, got \(value)")
    }
}

/// Convert CBOR map to JSON Data
func cborMapToJSON(_ value: CBOR) throws -> Data {
    guard case .map(let dict) = value else {
        throw PluginRuntimeError.handlerError("Expected CBOR map")
    }

    var swiftDict: [String: Any] = [:]
    for (key, val) in dict {
        guard case .utf8String(let keyStr) = key else {
            throw PluginRuntimeError.handlerError("Map key must be string")
        }
        swiftDict[keyStr] = cborToAny(val)
    }
    return try JSONSerialization.data(withJSONObject: swiftDict)
}

/// Convert CBOR to Any for JSON serialization
func cborToAny(_ value: CBOR) -> Any {
    switch value {
    case .unsignedInt(let n):
        return Int(n)
    case .negativeInt(let n):
        return -Int(n) - 1
    case .utf8String(let s):
        return s
    case .byteString(let b):
        return Data(b)
    case .array(let arr):
        return arr.map { cborToAny($0) }
    case .map(let m):
        var dict: [String: Any] = [:]
        for (k, v) in m {
            if case .utf8String(let key) = k {
                dict[key] = cborToAny(v)
            }
        }
        return dict
    case .boolean(let b):
        return b
    case .null:
        return NSNull()
    case .float(let f):
        return f
    case .double(let d):
        return d
    default:
        return NSNull()
    }
}

/// Parse JSON input from CBOR value (handles both map and byteString)
func parseJSONInput(_ value: CBOR) throws -> [String: Any] {
    let jsonData: Data
    if case .map = value {
        jsonData = try cborMapToJSON(value)
    } else {
        jsonData = try cborToData(value)
    }
    return try JSONSerialization.jsonObject(with: jsonData) as! [String: Any]
}

// MARK: - Op Implementations

// === STREAMING OPS (no accumulation) ===

struct EchoOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let payload = try input.collectAllBytes()
        try req.output().write(payload)
    }
    func metadata() -> OpMetadata { OpMetadata.builder("EchoOp").build() }
}

struct BinaryEchoOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let payload = try input.collectAllBytes()
        try req.output().write(payload)
    }
    func metadata() -> OpMetadata { OpMetadata.builder("BinaryEchoOp").build() }
}

struct PeerEchoOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let payload = try input.collectAllBytes()

        // Call host's echo capability
        let call = try req.peer().call(capUrn: "cap:in=media:;out=media:")
        let arg = call.arg(mediaUrn: "media:customer-message;textable")
        try arg.write(payload)
        try arg.close()

        let response = try call.finish(emitter: req.output(), base: 0.0, weight: 1.0)
        let responseBytes = try response.collectBytes()
        try req.output().write(responseBytes)
    }
    func metadata() -> OpMetadata { OpMetadata.builder("PeerEchoOp").build() }
}

// === ACCUMULATING OPS ===

struct DoubleOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let value = try firstValue(from: input)
        let json = try parseJSONInput(value)
        let inputValue = json["value"] as! Int
        let result = inputValue * 2
        try req.output().emitCbor(CBOR.unsignedInt(UInt64(result)))
    }
    func metadata() -> OpMetadata { OpMetadata.builder("DoubleOp").build() }
}

struct StreamChunksOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let value = try firstValue(from: input)
        let json = try parseJSONInput(value)
        let count = json["value"] as! Int

        for i in 0..<count {
            let chunk = "chunk-\(i)".data(using: .utf8)!
            try req.output().emitCbor(CBOR.byteString([UInt8](chunk)))
        }
        try req.output().emitCbor(CBOR.byteString([UInt8]("done".data(using: .utf8)!)))
    }
    func metadata() -> OpMetadata { OpMetadata.builder("StreamChunksOp").build() }
}

struct SlowResponseOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let value = try firstValue(from: input)
        let json = try parseJSONInput(value)
        let sleepMs = json["value"] as! Int

        try await Task.sleep(nanoseconds: UInt64(sleepMs) * 1_000_000)

        let response = "slept-\(sleepMs)ms"
        try req.output().emitCbor(CBOR.byteString([UInt8](response.data(using: .utf8)!)))
    }
    func metadata() -> OpMetadata { OpMetadata.builder("SlowResponseOp").build() }
}

struct GenerateLargeOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let value = try firstValue(from: input)
        let json = try parseJSONInput(value)
        let size = json["value"] as! Int

        // Generate pattern block efficiently (8KB block for fast generation)
        let pattern: [UInt8] = [65, 66, 67, 68, 69, 70, 71, 72] // "ABCDEFGH"
        let blockSize = 8 * 1024 // 8KB block
        var block = Data(capacity: blockSize)
        for i in 0..<blockSize {
            block.append(pattern[i % pattern.count])
        }

        // Write in 256KB chunks (matching protocol chunk size) for streaming
        let chunkSize = 256 * 1024
        var remaining = size
        var offset = 0

        while remaining > 0 {
            let currentChunkSize = min(chunkSize, remaining)
            var chunk = Data(capacity: currentChunkSize)

            // Build chunk from pre-generated blocks
            var chunkRemaining = currentChunkSize
            while chunkRemaining > 0 {
                let copySize = min(blockSize, chunkRemaining)
                chunk.append(block[0..<copySize])
                chunkRemaining -= copySize
            }

            try req.output().write(chunk)
            remaining -= currentChunkSize
            offset += currentChunkSize
        }
    }
    func metadata() -> OpMetadata { OpMetadata.builder("GenerateLargeOp").build() }
}

struct WithStatusOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let value = try firstValue(from: input)
        let json = try parseJSONInput(value)
        let steps = json["value"] as! Int

        for i in 0..<steps {
            let status = "step \(i)"
            req.output().log(level: "info", message: "processing: \(status)")
            try await Task.sleep(nanoseconds: 10_000_000) // 10ms
        }
        try req.output().emitCbor(CBOR.byteString([UInt8]("completed".data(using: .utf8)!)))
    }
    func metadata() -> OpMetadata { OpMetadata.builder("WithStatusOp").build() }
}

struct ThrowErrorOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let value = try firstValue(from: input)
        let json = try parseJSONInput(value)
        let message = json["value"] as! String
        throw NSError(domain: "InteropTestError", code: 1, userInfo: [NSLocalizedDescriptionKey: message])
    }
    func metadata() -> OpMetadata { OpMetadata.builder("ThrowErrorOp").build() }
}

struct NestedCallOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let value = try firstValue(from: input)
        let json = try parseJSONInput(value)
        let inputValue = json["value"] as! Int

        // Call host's double capability
        let inputData = try JSONSerialization.data(withJSONObject: ["value": inputValue])
        let call = try req.peer().call(capUrn: "cap:in=*;op=double;out=*")
        let arg = call.arg(mediaUrn: "media:order-value;json;textable;record")
        try arg.write(inputData)
        try arg.close()

        let response = try call.finish(emitter: req.output(), base: 0.0, weight: 1.0)
        let responseCbor = try response.collectValue()

        // Extract integer from response
        let hostResult: Int
        switch responseCbor {
        case .unsignedInt(let val):
            hostResult = Int(val)
        case .negativeInt(let val):
            hostResult = -Int(val) - 1
        default:
            throw PluginRuntimeError.handlerError("Expected integer from double")
        }

        // Double again locally
        let finalResult = hostResult * 2
        let finalData = try JSONSerialization.data(withJSONObject: finalResult, options: .fragmentsAllowed)
        try req.output().write(finalData)
    }
    func metadata() -> OpMetadata { OpMetadata.builder("NestedCallOp").build() }
}

struct HeartbeatStressOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let value = try firstValue(from: input)
        let json = try parseJSONInput(value)
        let durationMs = json["value"] as! Int

        try await Task.sleep(nanoseconds: UInt64(durationMs) * 1_000_000)

        let response = "stressed-\(durationMs)ms"
        try req.output().emitCbor(CBOR.byteString([UInt8](response.data(using: .utf8)!)))
    }
    func metadata() -> OpMetadata { OpMetadata.builder("HeartbeatStressOp").build() }
}

struct ConcurrentStressOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let value = try firstValue(from: input)
        let json = try parseJSONInput(value)
        let workUnits = json["value"] as! Int

        // Simulate concurrent work with arithmetic (matches existing Swift behavior)
        var sum: UInt64 = 0
        for i in 0..<(workUnits * 1000) {
            sum = sum &+ UInt64(i)
        }

        let response = "computed-\(sum)"
        try req.output().emitCbor(CBOR.byteString([UInt8](response.data(using: .utf8)!)))
    }
    func metadata() -> OpMetadata { OpMetadata.builder("ConcurrentStressOp").build() }
}

struct GetManifestOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        _ = try input.collectAllBytes() // drain void input
        let manifest = buildManifest()
        let resultData = try JSONSerialization.data(withJSONObject: manifest)
        try req.output().write(resultData)
    }
    func metadata() -> OpMetadata { OpMetadata.builder("GetManifestOp").build() }
}

struct ProcessLargeOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let payload = try input.collectAllBytes()
        let size = payload.count
        let hash = SHA256.hash(data: payload)
        let checksum = hash.compactMap { String(format: "%02x", $0) }.joined()

        let result: [String: Any] = [
            "size": size,
            "checksum": checksum
        ]
        let resultData = try JSONSerialization.data(withJSONObject: result)
        try req.output().write(resultData)
    }
    func metadata() -> OpMetadata { OpMetadata.builder("ProcessLargeOp").build() }
}

struct HashIncomingOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let payload = try input.collectAllBytes()
        let hash = SHA256.hash(data: payload)
        let checksum = hash.compactMap { String(format: "%02x", $0) }.joined()
        try req.output().emitCbor(CBOR.byteString([UInt8](checksum.data(using: .utf8)!)))
    }
    func metadata() -> OpMetadata { OpMetadata.builder("HashIncomingOp").build() }
}

struct VerifyBinaryOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        let payload = try input.collectAllBytes()
        var present = Set<UInt8>()

        for byte in payload {
            present.insert(byte)
        }

        if present.count == 256 {
            try req.output().emitCbor(CBOR.byteString([UInt8]("ok".data(using: .utf8)!)))
        } else {
            let missing = (0...255).filter { !present.contains(UInt8($0)) }
            let message = "missing \(missing.count) values"
            try req.output().emitCbor(CBOR.byteString([UInt8](message.data(using: .utf8)!)))
        }
    }
    func metadata() -> OpMetadata { OpMetadata.builder("VerifyBinaryOp").build() }
}

struct ReadFileInfoOp: Op, Sendable {
    typealias Output = Void
    func perform(dry: DryContext, wet: WetContext) async throws {
        let req = try wet.getRequired(CborRequest.self, for: WET_KEY_REQUEST)
        let input = try req.takeInput()
        // Payload is already file bytes (auto-converted by runtime from file-path)
        let payload = try input.collectAllBytes()
        let size = payload.count
        let hash = SHA256.hash(data: payload)
        let checksum = hash.compactMap { String(format: "%02x", $0) }.joined()

        let result: [String: Any] = [
            "size": size,
            "checksum": checksum
        ]
        let resultData = try JSONSerialization.data(withJSONObject: result)
        try req.output().write(resultData)
    }
    func metadata() -> OpMetadata { OpMetadata.builder("ReadFileInfoOp").build() }
}

// MARK: - Main

let manifestJSON = buildManifestJSON()
let runtime = PluginRuntime(manifestJSON: manifestJSON)

// Register all handlers as Op types
runtime.register_op_type(capUrn: "cap:in=\"media:\";op=echo;out=\"media:\"", make: EchoOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:order-value;json;textable;record\";op=double;out=\"media:loyalty-points;integer;textable;numeric\"", make: DoubleOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:update-count;json;textable;record\";op=stream_chunks;out=\"media:order-updates;textable\"", make: StreamChunksOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:product-image\";op=binary_echo;out=\"media:product-image\"", make: BinaryEchoOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:payment-delay-ms;json;textable;record\";op=slow_response;out=\"media:payment-result;textable\"", make: SlowResponseOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:report-size;json;textable;record\";op=generate_large;out=\"media:sales-report\"", make: GenerateLargeOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:fulfillment-steps;json;textable;record\";op=with_status;out=\"media:fulfillment-status;textable\"", make: WithStatusOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:payment-error;json;textable;record\";op=throw_error;out=\"media:void\"", make: ThrowErrorOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:customer-message;textable\";op=peer_echo;out=\"media:customer-message;textable\"", make: PeerEchoOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:order-value;json;textable;record\";op=nested_call;out=\"media:final-price;integer;textable;numeric\"", make: NestedCallOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:monitoring-duration-ms;json;textable;record\";op=heartbeat_stress;out=\"media:health-status;textable\"", make: HeartbeatStressOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:order-batch-size;json;textable;record\";op=concurrent_stress;out=\"media:batch-result;textable\"", make: ConcurrentStressOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:void\";op=get_manifest;out=\"media:service-capabilities;json;textable;record\"", make: GetManifestOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:uploaded-document\";op=process_large;out=\"media:document-info;json;textable;record\"", make: ProcessLargeOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:uploaded-document\";op=hash_incoming;out=\"media:document-hash;textable\"", make: HashIncomingOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:package-data\";op=verify_binary;out=\"media:verification-status;textable\"", make: VerifyBinaryOp.init)
runtime.register_op_type(capUrn: "cap:in=\"media:invoice;file-path;textable\";op=read_file_info;out=\"media:invoice-metadata;json;textable;record\"", make: ReadFileInfoOp.init)

try! runtime.run()
