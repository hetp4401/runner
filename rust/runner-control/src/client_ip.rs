use axum::http::HeaderMap;
use std::{collections::BTreeMap, net::IpAddr};
#[derive(Clone)]
pub struct ProxyConfig {
    header: String,
    trusted: Vec<(IpAddr, u8)>,
}
fn normalize(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        ip => ip,
    }
}
impl ProxyConfig {
    pub fn new(env: &BTreeMap<String, String>) -> anyhow::Result<Self> {
        let header = env
            .get("CLIENT_IP_HEADER")
            .map(String::as_str)
            .unwrap_or("cf-connecting-ip")
            .trim()
            .to_lowercase();
        anyhow::ensure!(
            ["cf-connecting-ip", "x-forwarded-for", "none"].contains(&header.as_str()),
            "invalid CLIENT_IP_HEADER"
        );
        let raw = env
            .get("TRUSTED_PROXIES")
            .map(String::as_str)
            .unwrap_or("127.0.0.0/8,::1/128");
        let mut trusted = vec![];
        for entry in raw
            .split(|c: char| c.is_whitespace() || c == ',')
            .filter(|s| !s.is_empty())
        {
            let mut parts = entry.split('/');
            let ip: IpAddr = parts
                .next()
                .unwrap()
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid TRUSTED_PROXIES address"))?;
            let max = if ip.is_ipv4() { 32 } else { 128 };
            let prefix = parts.next().map(|s| s.parse()).transpose()?.unwrap_or(max);
            anyhow::ensure!(
                prefix <= max && parts.next().is_none(),
                "invalid TRUSTED_PROXIES prefix"
            );
            trusted.push((ip, prefix));
        }
        Ok(Self { header, trusted })
    }
    pub fn trusted(&self, address: IpAddr) -> bool {
        self.trusted
            .iter()
            .any(|(network, prefix)| match (address, *network) {
                (IpAddr::V4(ip), IpAddr::V4(net)) => {
                    let mask = u32::MAX.checked_shl(32 - *prefix as u32).unwrap_or(0);
                    u32::from(ip) & mask == u32::from(net) & mask
                }
                (IpAddr::V6(ip), IpAddr::V6(net)) => {
                    let mask = u128::MAX.checked_shl(128 - *prefix as u32).unwrap_or(0);
                    u128::from(ip) & mask == u128::from(net) & mask
                }
                _ => false,
            })
    }
    pub fn address(&self, remote: IpAddr, headers: &HeaderMap) -> String {
        let remote = normalize(remote);
        if self.header == "none" || !self.trusted(remote) {
            return remote.to_string();
        }
        let Some(header) = headers.get(&self.header).and_then(|v| v.to_str().ok()) else {
            return remote.to_string();
        };
        if self.header == "cf-connecting-ip" {
            return header
                .trim()
                .parse::<IpAddr>()
                .map(normalize)
                .unwrap_or(remote)
                .to_string();
        }
        let chain = header
            .split(',')
            .map(|s| s.trim().parse::<IpAddr>().map(normalize))
            .collect::<Result<Vec<_>, _>>();
        let Ok(chain) = chain else {
            return remote.to_string();
        };
        let mut ip = remote;
        for hop in chain.into_iter().rev() {
            if !self.trusted(ip) {
                break;
            }
            ip = hop;
        }
        ip.to_string()
    }
}
