use capdag::{
    ArgSource, Cap, CapArg, CapManifest, CapOutput, CapUrn, CapUrnBuilder,
    OutputStream, PluginRuntime, RuntimeError, Request, WET_KEY_REQUEST,
    Op, OpMetadata, DryContext, WetContext, OpResult, OpError, async_trait,
    CAP_IDENTITY,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

// Request types
#[derive(Deserialize)]
struct ValueRequest {
    value: serde_json::Value,
}

// Helper: get Request from WetContext
fn get_req(wet: &mut WetContext) -> Result<Arc<Request>, OpError> {
    wet.get_required::<Request>(WET_KEY_REQUEST)
        .map_err(|e| OpError::ExecutionFailed(e.to_string()))
}

// Helper: take input and collect all bytes (async)
async fn collect_input_bytes(req: &Request) -> Result<Vec<u8>, OpError> {
    let input = req.take_input()
        .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
    input.collect_all_bytes().await
        .map_err(|e| OpError::ExecutionFailed(format!("Stream error: {}", e)))
}

// Helper: collect input and parse as JSON (async)
async fn collect_json(req: &Request) -> Result<ValueRequest, OpError> {
    let bytes = collect_input_bytes(req).await?;
    serde_json::from_slice(&bytes)
        .map_err(|e| OpError::ExecutionFailed(format!("Invalid JSON: {}", e)))
}

// Helper: emit CBOR value
fn emit(output: &OutputStream, value: &ciborium::Value) -> OpResult<()> {
    output.emit_cbor(value)
        .map_err(|e| OpError::ExecutionFailed(e.to_string()))
}

fn build_manifest() -> CapManifest {
    let caps = vec![
        // CAP_IDENTITY (required) - use from_string to parse the bare "cap:" constant
        Cap::new(
            CapUrn::from_string(CAP_IDENTITY)
                .unwrap(),
            "Identity".to_string(),
            "identity".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "echo")
                .in_spec("media:")
                .out_spec("media:")
                .build()
                .unwrap(),
            "Echo".to_string(),
            "echo".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "double")
                .in_spec("media:order-value;json;textable;record")
                .out_spec("media:loyalty-points;integer;textable;numeric")
                .build()
                .unwrap(),
            "Double".to_string(),
            "double".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "stream_chunks")
                .in_spec("media:update-count;json;textable;record")
                .out_spec("media:order-updates;textable")
                .build()
                .unwrap(),
            "Stream Chunks".to_string(),
            "stream_chunks".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "binary_echo")
                .in_spec("media:product-image")
                .out_spec("media:product-image")
                .build()
                .unwrap(),
            "Binary Echo".to_string(),
            "binary_echo".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "slow_response")
                .in_spec("media:payment-delay-ms;json;textable;record")
                .out_spec("media:payment-result;textable")
                .build()
                .unwrap(),
            "Slow Response".to_string(),
            "slow_response".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "generate_large")
                .in_spec("media:report-size;json;textable;record")
                .out_spec("media:sales-report")
                .build()
                .unwrap(),
            "Generate Large".to_string(),
            "generate_large".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "with_status")
                .in_spec("media:fulfillment-steps;json;textable;record")
                .out_spec("media:fulfillment-status;textable")
                .build()
                .unwrap(),
            "With Status".to_string(),
            "with_status".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "throw_error")
                .in_spec("media:payment-error;json;textable;record")
                .out_spec("media:void")
                .build()
                .unwrap(),
            "Throw Error".to_string(),
            "throw_error".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "peer_echo")
                .in_spec("media:customer-message;textable")
                .out_spec("media:customer-message;textable")
                .build()
                .unwrap(),
            "Peer Echo".to_string(),
            "peer_echo".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "nested_call")
                .in_spec("media:order-value;json;textable;record")
                .out_spec("media:final-price;integer;textable;numeric")
                .build()
                .unwrap(),
            "Nested Call".to_string(),
            "nested_call".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "heartbeat_stress")
                .in_spec("media:monitoring-duration-ms;json;textable;record")
                .out_spec("media:health-status;textable")
                .build()
                .unwrap(),
            "Heartbeat Stress".to_string(),
            "heartbeat_stress".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "concurrent_stress")
                .in_spec("media:order-batch-size;json;textable;record")
                .out_spec("media:batch-result;textable")
                .build()
                .unwrap(),
            "Concurrent Stress".to_string(),
            "concurrent_stress".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "get_manifest")
                .in_spec("media:void")
                .out_spec("media:service-capabilities;json;textable;record")
                .build()
                .unwrap(),
            "Get Manifest".to_string(),
            "get_manifest".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "process_large")
                .in_spec("media:uploaded-document")
                .out_spec("media:document-info;json;textable;record")
                .build()
                .unwrap(),
            "Process Large".to_string(),
            "process_large".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "hash_incoming")
                .in_spec("media:uploaded-document")
                .out_spec("media:document-hash;textable")
                .build()
                .unwrap(),
            "Hash Incoming".to_string(),
            "hash_incoming".to_string(),
        ),
        Cap::new(
            CapUrnBuilder::new()
                .tag("op", "verify_binary")
                .in_spec("media:package-data")
                .out_spec("media:verification-status;textable")
                .build()
                .unwrap(),
            "Verify Binary".to_string(),
            "verify_binary".to_string(),
        ),
        {
            let mut cap = Cap::new(
                CapUrnBuilder::new()
                    .tag("op", "read_file_info")
                    .in_spec("media:invoice;file-path;textable")
                    .out_spec("media:invoice-metadata;json;textable;record")
                    .build()
                    .unwrap(),
                "Read File Info".to_string(),
                "read_file_info".to_string(),
            );
            cap.args = vec![CapArg {
                media_urn: "media:invoice;file-path;textable".to_string(),
                required: true,
                sources: vec![
                    ArgSource::Stdin {
                        stdin: "media:".to_string(),
                    },
                    ArgSource::Position { position: 0 },
                ],
                arg_description: Some("Path to invoice file".to_string()),
                default_value: None,
                metadata: None,
            }];
            cap.output = Some(CapOutput {
                media_urn: "media:invoice-metadata;json;textable;record".to_string(),
                output_description: "Invoice file size and SHA256 checksum".to_string(),
                metadata: None,
            });
            cap
        },
    ];

    CapManifest::new(
        "InteropTestPlugin".to_string(),
        "1.0.0".to_string(),
        "Interoperability testing plugin (Rust)".to_string(),
        caps,
    )
}

// =============================================================================
// Op Implementations
// =============================================================================

// === STREAMING OPS (no accumulation) ===

#[derive(Default)]
struct EchoOp;
#[async_trait]
impl Op<()> for EchoOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let mut input = req.take_input().map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        while let Some(stream_result) = input.recv().await {
            let mut stream = stream_result.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            while let Some(chunk_result) = stream.recv().await {
                let chunk = chunk_result.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                emit(req.output(), &chunk)?;
            }
        }
        Ok(())
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("EchoOp").build() }
}

#[derive(Default)]
struct BinaryEchoOp;
#[async_trait]
impl Op<()> for BinaryEchoOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let mut input = req.take_input().map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        while let Some(stream_result) = input.recv().await {
            let mut stream = stream_result.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
            while let Some(chunk_result) = stream.recv().await {
                let chunk = chunk_result.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
                emit(req.output(), &chunk)?;
            }
        }
        Ok(())
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("BinaryEchoOp").build() }
}

#[derive(Default)]
struct PeerEchoOp;
#[async_trait]
impl Op<()> for PeerEchoOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        eprintln!("[peer_echo] Handler started");
        let payload = collect_input_bytes(&req).await?;
        eprintln!("[peer_echo] Collected {} bytes, calling peer", payload.len());

        let response = req.peer().call_with_bytes(
            "cap:in=media:;out=media:",
            &[("media:customer-message;textable", &payload)],
        ).await.map_err(|e| {
            eprintln!("[peer_echo] Peer call failed: {}", e);
            OpError::ExecutionFailed(e.to_string())
        })?;

        eprintln!("[peer_echo] Got peer response stream");
        let value = response.collect_value().await
            .map_err(|e| OpError::ExecutionFailed(format!("Peer response error: {}", e)))?;
        eprintln!("[peer_echo] Got peer response value: {:?}", value);
        emit(req.output(), &value)
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("PeerEchoOp").build() }
}

// === ACCUMULATING OPS ===

#[derive(Default)]
struct DoubleOp;
#[async_trait]
impl Op<()> for DoubleOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        eprintln!("[double] Handler starting");
        let json_req = collect_json(&req).await?;
        let value = json_req.value.as_u64()
            .ok_or_else(|| OpError::ExecutionFailed("Expected number".to_string()))?;
        eprintln!("[double] Parsed value: {}, doubling to: {}", value, value * 2);
        let result = value * 2;
        emit(req.output(), &ciborium::Value::Integer(result.into()))?;
        eprintln!("[double] Handler complete");
        Ok(())
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("DoubleOp").build() }
}

#[derive(Default)]
struct StreamChunksOp;
#[async_trait]
impl Op<()> for StreamChunksOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let json_req = collect_json(&req).await?;
        let count = json_req.value.as_u64()
            .ok_or_else(|| OpError::ExecutionFailed("Expected number".to_string()))?;
        for i in 0..count {
            let chunk = format!("chunk-{}", i);
            req.output().write(chunk.as_bytes())
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        }
        req.output().write(b"done")
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        Ok(())
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("StreamChunksOp").build() }
}

#[derive(Default)]
struct SlowResponseOp;
#[async_trait]
impl Op<()> for SlowResponseOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let json_req = collect_json(&req).await?;
        let sleep_ms = json_req.value.as_u64()
            .ok_or_else(|| OpError::ExecutionFailed("Expected number".to_string()))?;
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        let response = format!("slept-{}ms", sleep_ms);
        req.output().write(response.as_bytes())
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        Ok(())
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("SlowResponseOp").build() }
}

#[derive(Default)]
struct GenerateLargeOp;
#[async_trait]
impl Op<()> for GenerateLargeOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let json_req = collect_json(&req).await?;
        let size = json_req.value.as_u64()
            .ok_or_else(|| OpError::ExecutionFailed("Expected number".to_string()))? as usize;
        let pattern = b"ABCDEFGH";
        let mut result = Vec::with_capacity(size);
        for i in 0..size {
            result.push(pattern[i % pattern.len()]);
        }
        emit(req.output(), &ciborium::Value::Bytes(result))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("GenerateLargeOp").build() }
}

#[derive(Default)]
struct WithStatusOp;
#[async_trait]
impl Op<()> for WithStatusOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let json_req = collect_json(&req).await?;
        let steps = json_req.value.as_u64()
            .ok_or_else(|| OpError::ExecutionFailed("Expected number".to_string()))?;
        for i in 0..steps {
            let status = format!("step {}", i);
            req.output().log("processing", &status);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        req.output().write(b"completed")
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        Ok(())
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("WithStatusOp").build() }
}

#[derive(Default)]
struct ThrowErrorOp;
#[async_trait]
impl Op<()> for ThrowErrorOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let json_req = collect_json(&req).await?;
        let message = json_req.value.as_str()
            .ok_or_else(|| OpError::ExecutionFailed("Expected string".to_string()))?;
        Err(OpError::ExecutionFailed(message.to_string()))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("ThrowErrorOp").build() }
}

#[derive(Default)]
struct NestedCallOp;
#[async_trait]
impl Op<()> for NestedCallOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        eprintln!("[nested_call] Starting handler");
        let json_req = collect_json(&req).await?;
        let value = json_req.value.as_u64()
            .ok_or_else(|| OpError::ExecutionFailed("Expected number".to_string()))?;
        eprintln!("[nested_call] Parsed value: {}", value);

        let double_arg = serde_json::to_vec(&serde_json::json!({"value": value}))
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;

        eprintln!("[nested_call] Calling peer double");
        let response = req.peer().call_with_bytes(
            r#"cap:in="media:order-value;json;textable;record";op=double;out="media:loyalty-points;integer;textable;numeric""#,
            &[("media:order-value;json;textable;record", &double_arg)],
        ).await.map_err(|e| OpError::ExecutionFailed(e.to_string()))?;

        let cbor_value = response.collect_value().await
            .map_err(|e| OpError::ExecutionFailed(format!("Peer response error: {}", e)))?;
        eprintln!("[nested_call] Peer response: {:?}", cbor_value);

        let host_result = match cbor_value {
            ciborium::Value::Integer(n) => {
                let val: i128 = n.into();
                val as u64
            }
            _ => return Err(OpError::ExecutionFailed(format!(
                "Expected integer from double, got: {:?}", cbor_value
            ))),
        };

        let final_result = host_result * 2;
        eprintln!("[nested_call] Final result: {}", final_result);
        emit(req.output(), &ciborium::Value::Integer(final_result.into()))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("NestedCallOp").build() }
}

#[derive(Default)]
struct HeartbeatStressOp;
#[async_trait]
impl Op<()> for HeartbeatStressOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let json_req = collect_json(&req).await?;
        let duration_ms = json_req.value.as_u64()
            .ok_or_else(|| OpError::ExecutionFailed("Expected number".to_string()))?;
        let chunks = duration_ms / 100;
        let remainder = duration_ms % 100;
        for _ in 0..chunks {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if remainder > 0 {
            tokio::time::sleep(Duration::from_millis(remainder)).await;
        }
        let response = format!("stressed-{}ms", duration_ms);
        req.output().write(response.as_bytes())
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        Ok(())
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("HeartbeatStressOp").build() }
}

#[derive(Default)]
struct ConcurrentStressOp;
#[async_trait]
impl Op<()> for ConcurrentStressOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let json_req = collect_json(&req).await?;
        let task_count = json_req.value.as_u64()
            .ok_or_else(|| OpError::ExecutionFailed("Expected number".to_string()))? as usize;
        let handles: Vec<_> = (0..task_count)
            .map(|i| {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    i
                })
            })
            .collect();
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        let sum: usize = results.iter().sum();
        let response = format!("computed-{}", sum);
        req.output().write(response.as_bytes())
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        Ok(())
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("ConcurrentStressOp").build() }
}

#[derive(Default)]
struct GetManifestOp;
#[async_trait]
impl Op<()> for GetManifestOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let _ = collect_input_bytes(&req).await;
        let manifest = build_manifest();
        let manifest_json = serde_json::to_vec(&manifest)
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        emit(req.output(), &ciborium::Value::Bytes(manifest_json))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("GetManifestOp").build() }
}

#[derive(Default)]
struct ProcessLargeOp;
#[async_trait]
impl Op<()> for ProcessLargeOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let payload = collect_input_bytes(&req).await?;
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let hash = hasher.finalize();
        let hash_hex = hex::encode(hash);
        let result = serde_json::to_vec(&serde_json::json!({"size": payload.len(), "checksum": hash_hex}))
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        emit(req.output(), &ciborium::Value::Bytes(result))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("ProcessLargeOp").build() }
}

#[derive(Default)]
struct HashIncomingOp;
#[async_trait]
impl Op<()> for HashIncomingOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let payload = collect_input_bytes(&req).await?;
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let hash = hasher.finalize();
        let hash_hex = hex::encode(hash);
        req.output().write(hash_hex.as_bytes())
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        Ok(())
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("HashIncomingOp").build() }
}

#[derive(Default)]
struct VerifyBinaryOp;
#[async_trait]
impl Op<()> for VerifyBinaryOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let payload = collect_input_bytes(&req).await?;
        let mut seen = HashSet::new();
        for &byte in &payload {
            seen.insert(byte);
        }
        if seen.len() == 256 {
            req.output().write(b"ok")
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        } else {
            let mut missing: Vec<u8> = (0..=255u8).filter(|b| !seen.contains(b)).collect();
            missing.sort();
            let msg = format!("missing byte values: {:?}", missing);
            req.output().write(msg.as_bytes())
                .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        }
        Ok(())
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("VerifyBinaryOp").build() }
}

#[derive(Default)]
struct ReadFileInfoOp;
#[async_trait]
impl Op<()> for ReadFileInfoOp {
    async fn perform(&self, _dry: &mut DryContext, wet: &mut WetContext) -> OpResult<()> {
        let req = get_req(wet)?;
        let file_content = collect_input_bytes(&req).await?;
        let mut hasher = Sha256::new();
        hasher.update(&file_content);
        let hash = hasher.finalize();
        let hash_hex = hex::encode(hash);
        let result = serde_json::to_vec(&serde_json::json!({"size": file_content.len(), "checksum": hash_hex}))
            .map_err(|e| OpError::ExecutionFailed(e.to_string()))?;
        emit(req.output(), &ciborium::Value::Bytes(result))
    }
    fn metadata(&self) -> OpMetadata { OpMetadata::builder("ReadFileInfoOp").build() }
}

#[tokio::main]
async fn main() -> Result<(), RuntimeError> {
    eprintln!("[PLUGIN MAIN] Starting");
    let manifest = build_manifest();
    eprintln!("[PLUGIN MAIN] Built manifest");
    let mut runtime = PluginRuntime::with_manifest(manifest);
    eprintln!("[PLUGIN MAIN] Created runtime");

    // Register all handlers as Op types
    runtime.register_op_type::<EchoOp>(r#"cap:in="media:";op=echo;out="media:""#);
    runtime.register_op_type::<DoubleOp>(r#"cap:in="media:order-value;json;textable;record";op=double;out="media:loyalty-points;integer;textable;numeric""#);
    runtime.register_op_type::<StreamChunksOp>(r#"cap:in="media:update-count;json;textable;record";op=stream_chunks;out="media:order-updates;textable""#);
    runtime.register_op_type::<BinaryEchoOp>(r#"cap:in="media:product-image";op=binary_echo;out="media:product-image""#);
    runtime.register_op_type::<SlowResponseOp>(r#"cap:in="media:payment-delay-ms;json;textable;record";op=slow_response;out="media:payment-result;textable""#);
    runtime.register_op_type::<GenerateLargeOp>(r#"cap:in="media:report-size;json;textable;record";op=generate_large;out="media:sales-report""#);
    runtime.register_op_type::<WithStatusOp>(r#"cap:in="media:fulfillment-steps;json;textable;record";op=with_status;out="media:fulfillment-status;textable""#);
    runtime.register_op_type::<ThrowErrorOp>(r#"cap:in="media:payment-error;json;textable;record";op=throw_error;out=media:void"#);
    runtime.register_op_type::<PeerEchoOp>(r#"cap:in="media:customer-message;textable";op=peer_echo;out="media:customer-message;textable""#);
    runtime.register_op_type::<NestedCallOp>(r#"cap:in="media:order-value;json;textable;record";op=nested_call;out="media:final-price;integer;textable;numeric""#);
    runtime.register_op_type::<HeartbeatStressOp>(
        r#"cap:in="media:monitoring-duration-ms;json;textable;record";op=heartbeat_stress;out="media:health-status;textable""#,
    );
    runtime.register_op_type::<ConcurrentStressOp>(
        r#"cap:in="media:order-batch-size;json;textable;record";op=concurrent_stress;out="media:batch-result;textable""#,
    );
    runtime.register_op_type::<GetManifestOp>(r#"cap:in=media:void;op=get_manifest;out="media:service-capabilities;json;textable;record""#);
    runtime.register_op_type::<ProcessLargeOp>(r#"cap:in="media:uploaded-document";op=process_large;out="media:document-info;json;textable;record""#);
    runtime.register_op_type::<HashIncomingOp>(r#"cap:in="media:uploaded-document";op=hash_incoming;out="media:document-hash;textable""#);
    runtime.register_op_type::<VerifyBinaryOp>(r#"cap:in="media:package-data";op=verify_binary;out="media:verification-status;textable""#);
    runtime.register_op_type::<ReadFileInfoOp>(r#"cap:in="media:invoice;file-path;textable";op=read_file_info;out="media:invoice-metadata;json;textable;record""#);

    eprintln!("[PLUGIN MAIN] Calling runtime.run()");
    let result = runtime.run().await;
    eprintln!("[PLUGIN MAIN] runtime.run() returned: {:?}", result);
    result
}
