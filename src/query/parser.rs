use anyhow::{anyhow, Result};
use sha3::{Digest, Keccak256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSignature {
    pub name: String,
    pub params: Vec<AbiParam>,
    pub selector: [u8; 4],
}

impl EventSignature {
    pub fn parse(sig: &str) -> Result<Self> {
        let sig = sig.trim();

        let open_paren = sig
            .find('(')
            .ok_or_else(|| anyhow!("Invalid signature: missing '('"))?;
        let close_paren = sig
            .rfind(')')
            .ok_or_else(|| anyhow!("Invalid signature: missing ')'"))?;

        if close_paren <= open_paren {
            return Err(anyhow!("Invalid signature: malformed parentheses"));
        }

        let name = sig[..open_paren].trim().to_string();
        if name.is_empty() {
            return Err(anyhow!("Invalid signature: empty event name"));
        }

        let params_str = &sig[open_paren + 1..close_paren];
        let params = parse_params(params_str)?;

        let canonical = Self::canonical_signature(&name, &params);
        let hash = Keccak256::digest(canonical.as_bytes());
        let selector: [u8; 4] = hash[0..4].try_into().unwrap();

        Ok(Self {
            name,
            params,
            selector,
        })
    }

    pub fn selector_hex(&self) -> String {
        hex::encode(self.selector)
    }

    fn canonical_signature(name: &str, params: &[AbiParam]) -> String {
        let param_types: Vec<String> = params.iter().map(|p| p.ty.canonical()).collect();
        format!("{}({})", name, param_types.join(","))
    }

    pub fn to_cte_sql(&self) -> String {
        let mut selects = Vec::new();
        let mut topic_idx = 1; // topic0 is the selector
        let mut data_offset = 0;

        for (i, param) in self.params.iter().enumerate() {
            let col_name = param
                .name
                .as_deref()
                .map(|n| n.to_string())
                .unwrap_or_else(|| format!("arg{}", i));

            let decode_expr = if param.indexed {
                let expr = param.ty.topic_decode_sql(topic_idx);
                topic_idx += 1;
                expr
            } else {
                let expr = param.ty.data_decode_sql(data_offset);
                data_offset += 32;
                expr
            };

            selects.push(format!("{} AS \"{}\"", decode_expr, col_name));
        }

        let select_clause = if selects.is_empty() {
            String::new()
        } else {
            format!(", {}", selects.join(", "))
        };

        format!(
            r#""{name}" AS (
    SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address{select_clause}
    FROM logs
    WHERE selector = '\x{selector}'
)"#,
            name = self.name,
            select_clause = select_clause,
            selector = self.selector_hex(),
        )
    }
}

fn parse_params(params_str: &str) -> Result<Vec<AbiParam>> {
    if params_str.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut params = Vec::new();
    let mut depth = 0;
    let mut current = String::new();

    for c in params_str.chars() {
        match c {
            '(' => {
                depth += 1;
                current.push(c);
            }
            ')' => {
                depth -= 1;
                current.push(c);
            }
            ',' if depth == 0 => {
                if !current.trim().is_empty() {
                    params.push(AbiParam::parse(current.trim())?);
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }

    if !current.trim().is_empty() {
        params.push(AbiParam::parse(current.trim())?);
    }

    Ok(params)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbiParam {
    pub name: Option<String>,
    pub ty: AbiType,
    pub indexed: bool,
}

impl AbiParam {
    pub fn parse(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split_whitespace().collect();

        if parts.is_empty() {
            return Err(anyhow!("Empty parameter"));
        }

        let (ty_str, indexed, name) = match parts.len() {
            1 => (parts[0], false, None),
            2 => {
                if parts[1] == "indexed" {
                    (parts[0], true, None)
                } else {
                    (parts[0], false, Some(parts[1].to_string()))
                }
            }
            3 => {
                if parts[1] == "indexed" {
                    (parts[0], true, Some(parts[2].to_string()))
                } else {
                    return Err(anyhow!("Invalid parameter format: {}", s));
                }
            }
            _ => return Err(anyhow!("Invalid parameter format: {}", s)),
        };

        let ty = AbiType::parse(ty_str)?;

        Ok(Self { name, ty, indexed })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbiType {
    Address,
    Bool,
    Uint(u16),
    Int(u16),
    Bytes(Option<u8>),
    String,
    FixedArray(Box<AbiType>, usize),
    DynamicArray(Box<AbiType>),
    Tuple(Vec<AbiType>),
}

impl AbiType {
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();

        if s == "address" {
            return Ok(AbiType::Address);
        }
        if s == "bool" {
            return Ok(AbiType::Bool);
        }
        if s == "string" {
            return Ok(AbiType::String);
        }
        if s == "bytes" {
            return Ok(AbiType::Bytes(None));
        }

        if let Some(rest) = s.strip_prefix("uint") {
            let bits: u16 = if rest.is_empty() {
                256
            } else {
                rest.parse().map_err(|_| anyhow!("Invalid uint size"))?
            };
            return Ok(AbiType::Uint(bits));
        }

        if let Some(rest) = s.strip_prefix("int") {
            let bits: u16 = if rest.is_empty() {
                256
            } else {
                rest.parse().map_err(|_| anyhow!("Invalid int size"))?
            };
            return Ok(AbiType::Int(bits));
        }

        if let Some(rest) = s.strip_prefix("bytes") {
            if rest.is_empty() {
                return Ok(AbiType::Bytes(None));
            }
            let size: u8 = rest
                .parse()
                .map_err(|_| anyhow!("Invalid bytes size: {}", rest))?;
            return Ok(AbiType::Bytes(Some(size)));
        }

        if s.ends_with("[]") {
            let inner = AbiType::parse(&s[..s.len() - 2])?;
            return Ok(AbiType::DynamicArray(Box::new(inner)));
        }

        if let Some(bracket_pos) = s.rfind('[') {
            if s.ends_with(']') {
                let inner = AbiType::parse(&s[..bracket_pos])?;
                let size_str = &s[bracket_pos + 1..s.len() - 1];
                let size: usize = size_str
                    .parse()
                    .map_err(|_| anyhow!("Invalid array size: {}", size_str))?;
                return Ok(AbiType::FixedArray(Box::new(inner), size));
            }
        }

        Err(anyhow!("Unknown ABI type: {}", s))
    }

    pub fn canonical(&self) -> String {
        match self {
            AbiType::Address => "address".to_string(),
            AbiType::Bool => "bool".to_string(),
            AbiType::Uint(bits) => format!("uint{}", bits),
            AbiType::Int(bits) => format!("int{}", bits),
            AbiType::Bytes(None) => "bytes".to_string(),
            AbiType::Bytes(Some(n)) => format!("bytes{}", n),
            AbiType::String => "string".to_string(),
            AbiType::FixedArray(inner, size) => format!("{}[{}]", inner.canonical(), size),
            AbiType::DynamicArray(inner) => format!("{}[]", inner.canonical()),
            AbiType::Tuple(types) => {
                let inner: Vec<String> = types.iter().map(|t| t.canonical()).collect();
                format!("({})", inner.join(","))
            }
        }
    }

    pub fn topic_decode_sql(&self, topic_idx: usize) -> String {
        match self {
            AbiType::Address => format!("abi_address(topics[{}])", topic_idx),
            AbiType::Uint(_) | AbiType::Int(_) => format!("abi_uint(topics[{}])", topic_idx),
            AbiType::Bool => format!("abi_bool(topics[{}])", topic_idx),
            AbiType::Bytes(Some(_)) | AbiType::Bytes(None) => format!("topics[{}]", topic_idx),
            _ => format!("topics[{}]", topic_idx),
        }
    }

    pub fn data_decode_sql(&self, offset: usize) -> String {
        let start = offset + 1;
        match self {
            AbiType::Address => format!("abi_address(substring(data FROM {} FOR 32))", start),
            AbiType::Uint(_) | AbiType::Int(_) => {
                format!("abi_uint(substring(data FROM {} FOR 32))", start)
            }
            AbiType::Bool => format!("abi_bool(substring(data FROM {} FOR 32))", start),
            AbiType::Bytes(Some(_)) | AbiType::Bytes(None) => {
                format!("substring(data FROM {} FOR 32)", start)
            }
            AbiType::String => format!("abi_string(data, {})", offset),
            _ => format!("substring(data FROM {} FOR 32)", start),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_transfer_signature() {
        let sig = EventSignature::parse("Transfer(address,address,uint256)").unwrap();
        assert_eq!(sig.name, "Transfer");
        assert_eq!(sig.params.len(), 3);
        assert_eq!(sig.params[0].ty, AbiType::Address);
        assert_eq!(sig.params[1].ty, AbiType::Address);
        assert_eq!(sig.params[2].ty, AbiType::Uint(256));
        assert_eq!(sig.selector_hex(), "ddf252ad");
    }

    #[test]
    fn test_parse_approval_signature() {
        let sig = EventSignature::parse("Approval(address,address,uint256)").unwrap();
        assert_eq!(sig.name, "Approval");
        assert_eq!(sig.selector_hex(), "8c5be1e5");
    }

    #[test]
    fn test_parse_indexed_params() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        assert_eq!(sig.name, "Transfer");
        assert_eq!(sig.params.len(), 3);
        assert!(sig.params[0].indexed);
        assert_eq!(sig.params[0].name.as_deref(), Some("from"));
        assert!(sig.params[1].indexed);
        assert_eq!(sig.params[1].name.as_deref(), Some("to"));
        assert!(!sig.params[2].indexed);
        assert_eq!(sig.params[2].name.as_deref(), Some("value"));
        assert_eq!(sig.selector_hex(), "ddf252ad");
    }

    #[test]
    fn test_parse_empty_params() {
        let sig = EventSignature::parse("Paused()").unwrap();
        assert_eq!(sig.name, "Paused");
        assert!(sig.params.is_empty());
    }

    #[test]
    fn test_cte_generation() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        let cte = sig.to_cte_sql();
        assert!(cte.contains("\"Transfer\""));
        assert!(cte.contains("abi_address(topics[1])"));
        assert!(cte.contains("abi_address(topics[2])"));
        assert!(cte.contains("abi_uint(substring(data FROM 1 FOR 32))"));
        assert!(cte.contains("ddf252ad"));
    }

    #[test]
    fn test_parse_bytes32() {
        let sig = EventSignature::parse("SomeEvent(bytes32)").unwrap();
        assert_eq!(sig.params[0].ty, AbiType::Bytes(Some(32)));
    }

    #[test]
    fn test_parse_dynamic_bytes() {
        let sig = EventSignature::parse("SomeEvent(bytes)").unwrap();
        assert_eq!(sig.params[0].ty, AbiType::Bytes(None));
    }

    #[test]
    fn test_canonical_signature() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        let canonical = EventSignature::canonical_signature(&sig.name, &sig.params);
        assert_eq!(canonical, "Transfer(address,address,uint256)");
    }
}
