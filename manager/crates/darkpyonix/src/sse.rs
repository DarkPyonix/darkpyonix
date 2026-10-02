//! Incremental Server-Sent Events parser for the manager's events stream.

use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct SseEvent {
    /// SSE `id` = kernel `seq`; absent for `replay_truncated`.
    pub id: Option<u64>,
    /// SSE `event` = PROTOCOL §3.4 type.
    pub kind: String,
    /// SSE `data` = the event's `data` object.
    pub data: Value,
}

#[derive(Default)]
pub struct SseParser {
    buf: Vec<u8>,
}

impl SseParser {
    /// Feed bytes; returns every complete event they finish.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<SseEvent> {
        self.buf.extend_from_slice(bytes);
        let mut out = Vec::new();
        while let Some((end, sep)) = find_blank_line(&self.buf) {
            let block: Vec<u8> = self.buf.drain(..end + sep).take(end).collect();
            if let Some(ev) = parse_block(&String::from_utf8_lossy(&block)) {
                out.push(ev);
            }
        }
        out
    }
}

fn find_blank_line(buf: &[u8]) -> Option<(usize, usize)> {
    (0..buf.len()).find_map(|i| {
        let rest = &buf[i..];
        if rest.starts_with(b"\r\n\r\n") {
            Some((i, 4))
        } else if rest.starts_with(b"\n\n") || rest.starts_with(b"\r\r") {
            Some((i, 2))
        } else {
            None
        }
    })
}

fn parse_block(block: &str) -> Option<SseEvent> {
    let mut id = None;
    let mut kind = String::from("message");
    let mut data: Vec<&str> = Vec::new();
    for line in block.lines() {
        if line.starts_with(':') || line.is_empty() {
            continue; // comment / keep-alive
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "id" => id = value.trim().parse().ok(),
            "event" => kind = value.to_string(),
            "data" => data.push(value),
            _ => {}
        }
    }
    if data.is_empty() {
        return None;
    }
    let joined = data.join("\n");
    let data = serde_json::from_str(&joined).unwrap_or(Value::String(joined));
    Some(SseEvent { id, kind, data })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_fr_m1_sse_frames_split_across_chunks() {
        let mut p = SseParser::default();
        assert!(p.push(b": keep-alive\n\nid: 7\nevent: out").is_empty());
        assert!(p.push(b"put\ndata: {\"run_id\":").is_empty());
        let evs = p.push(b"\"r\"}\n\n");
        assert_eq!(evs, vec![SseEvent { id: Some(7), kind: "output".into(), data: json!({"run_id": "r"}) }]);
    }

    #[test]
    fn test_fr_m1_sse_parses_id_event_data() {
        let mut p = SseParser::default();
        let evs = p.push(
            b"id: 1043\nevent: output\ndata: {\"run_id\":\"r1\",\"index\":3}\n\n\
              event: replay_truncated\r\ndata: {\"oldest_seq\":5}\r\n\r\nid: 1044\n",
        );
        assert_eq!(
            evs,
            vec![
                SseEvent { id: Some(1043), kind: "output".into(), data: json!({"run_id": "r1", "index": 3}) },
                SseEvent { id: None, kind: "replay_truncated".into(), data: json!({"oldest_seq": 5}) },
            ]
        );
        let evs = p.push(b"event: run.finished\ndata: {\"status\":\"ok\"}\n\n");
        assert_eq!(evs[0].id, Some(1044));
        assert_eq!(evs[0].kind, "run.finished");
    }
}
