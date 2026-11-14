use crate::{
    control::{Control, n, s},
    error::http,
    model::*,
    recovery::array,
};
use base64::Engine;
use reqwest::Method;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
fn tunnel_secret() -> String {
    let mut bytes = Vec::from(uuid::Uuid::new_v4().into_bytes());
    bytes.extend(uuid::Uuid::new_v4().into_bytes());
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
impl Control {
    pub async fn cf(
        &mut self,
        path: &str,
        method: Method,
        body: Option<Value>,
    ) -> anyhow::Result<Value> {
        let write = method != Method::GET;
        let authority = self.store.authority.clone();
        if write {
            if let Some((authority, proof)) = &authority {
                authority.check(Some(proof)).await?;
            }
        }
        let base = if self.e("CF_API_BASE").is_empty() {
            "https://api.cloudflare.com/client/v4"
        } else {
            self.e("CF_API_BASE")
        };
        let mut request = self.http.request(method, format!("{base}{path}"));
        request = if !self.e("CF_API_TOKEN").is_empty() {
            request.bearer_auth(self.e("CF_API_TOKEN"))
        } else {
            request
                .header("x-auth-email", self.e("CF_API_EMAIL"))
                .header("x-auth-key", self.e("CF_API_KEY"))
        };
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|_| http(502, "Cloudflare network request failed"))?;
        let status = response.status();
        let data: Value = runner_core::response_json(response, 4 << 20)
            .await
            .map_err(|_| {
                http(
                    502,
                    format!("Cloudflare API request failed (HTTP {})", status.as_u16()),
                )
            })?;
        if data["success"] != true {
            return Err(http(
                502,
                format!(
                    "Cloudflare API {}: request rejected",
                    path.split('?').next().unwrap_or("")
                ),
            ));
        }
        if write {
            if let Some((authority, proof)) = &authority {
                authority.check(Some(proof)).await?;
            }
        }
        Ok(data)
    }
    pub fn public_tunnel_name(&self, name: &str) -> String {
        format!(
            "{}-app-{}",
            self.e("TUNNEL_PREFIX"),
            &runner_core::auth::sha256(name)[..20]
        )
    }
    pub async fn recover_tunnels(&mut self, now: u64) -> anyhow::Result<()> {
        if self.tunnels_ready {
            return Ok(());
        }
        let account = self.e("ACCOUNT_ID").to_owned();
        let prefix = self.e("TUNNEL_PREFIX").to_owned();
        let names = self
            .projects
            .keys()
            .map(|name| (self.public_tunnel_name(name), name.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut found = BTreeMap::new();
        let mut public = BTreeMap::new();
        let mut public_ids = BTreeSet::new();
        for page in 1..=1000 {
            let data = self
                .cf(
                    &format!(
                        "/accounts/{account}/cfd_tunnel?is_deleted=false&per_page=1000&page={page}"
                    ),
                    Method::GET,
                    None,
                )
                .await?;
            let tunnels = data["result"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("Cloudflare returned an invalid tunnel listing"))?;
            for t in tunnels {
                let name = s(t, "name");
                if !t["deleted_at"].is_null() {
                    continue;
                }
                let id = s(t, "id");
                if name
                    .strip_prefix(&format!("{prefix}-app-"))
                    .is_some_and(|s| {
                        s.len() == 20
                            && s.bytes()
                                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                    })
                {
                    anyhow::ensure!(!id.is_empty(), "Cloudflare returned an invalid app tunnel");
                    anyhow::ensure!(
                        s(t, "config_src").is_empty() || t["config_src"] == "local",
                        "shared app tunnels must use local agent ingress"
                    );
                    if let Some(app) = names.get(name) {
                        anyhow::ensure!(
                            !public.contains_key(app),
                            "duplicate Cloudflare application tunnel names"
                        );
                        public.insert(app.clone(), json!({"id":id,"token":""}));
                    }
                    public_ids.insert(id.to_owned());
                }
                if let Some(number) = name
                    .strip_prefix(&format!("{prefix}-"))
                    .filter(|s| {
                        !s.is_empty()
                            && !s.starts_with('0')
                            && s.bytes().all(|c| c.is_ascii_digit())
                    })
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    anyhow::ensure!(
                        number < 1 << 53 && !id.is_empty(),
                        "Cloudflare returned an invalid runner tunnel"
                    );
                    anyhow::ensure!(
                        !found
                            .get(&number)
                            .is_some_and(|v: &Value| v["tunnel"] != id),
                        "duplicate Cloudflare runner tunnel names"
                    );
                    found.insert(number, json!({"n":number,"tunnel":id,"token":""}));
                }
            }
            let per_page = n(&data["result_info"], "per_page").max(1);
            let per_page = if per_page == 1 { 1000 } else { per_page };
            let pages = n(&data["result_info"], "total_pages")
                .max(n(&data["result_info"], "total_count").div_ceil(per_page));
            if pages > 0 && page >= pages || pages == 0 && tunnels.len() < per_page as usize {
                break;
            }
            anyhow::ensure!(
                !tunnels.is_empty() && page < 1000,
                "Cloudflare returned an incomplete tunnel listing"
            );
        }
        for (name, tunnel) in &mut public {
            let Some(p) = self.projects.get(name).filter(|p| p.enabled > 0) else {
                continue;
            };
            if self
                .version(name, p.version)?
                .is_none_or(|s| s.port.is_none())
            {
                continue;
            }
            let token = self
                .cf(
                    &format!("/accounts/{account}/cfd_tunnel/{}/token", s(tunnel, "id")),
                    Method::GET,
                    None,
                )
                .await?["result"]
                .clone();
            anyhow::ensure!(
                token.as_str().is_some_and(|s| !s.is_empty()),
                "Cloudflare returned no app tunnel token"
            );
            tunnel["token"] = token;
        }
        self.store.exec("DELETE FROM slots", &[])?;
        for slot in found.values() {
            self.store.exec(
                "INSERT INTO slots(n,tunnel,token,created) VALUES(?,?,?,?)",
                &[
                    slot["n"].clone(),
                    slot["tunnel"].clone(),
                    json!(""),
                    json!(now),
                ],
            )?;
        }
        self.slots = found;
        self.public_tunnels = public;
        self.public_tunnel_ids = public_ids;
        self.tunnels_ready = true;
        self.tunnel_recovery_error = None;
        self.dns_dirty = true;
        Ok(())
    }
    pub async fn tunnel_for(&mut self, number: u64, now: u64) -> anyhow::Result<Value> {
        if !self.tunnels_ready {
            return Err(http(503, "recovering Cloudflare tunnels; retry shortly"));
        }
        if let Some(slot) = self
            .slots
            .get(&number)
            .filter(|t| !s(t, "token").is_empty())
        {
            return Ok(slot.clone());
        }
        let account = self.e("ACCOUNT_ID").to_owned();
        let name = format!("{}-{number}", self.e("TUNNEL_PREFIX"));
        let mut tunnel = self.slots.get(&number).map(|t| json!({"id":t["tunnel"]}));
        if tunnel.is_none() {
            let data = self
                .cf(
                    &format!("/accounts/{account}/cfd_tunnel?name={name}&is_deleted=false"),
                    Method::GET,
                    None,
                )
                .await?;
            let matches = array(&data["result"])
                .iter()
                .filter(|t| t["name"] == name && t["deleted_at"].is_null())
                .collect::<Vec<_>>();
            anyhow::ensure!(
                matches.len() <= 1,
                "duplicate Cloudflare runner tunnel names"
            );
            tunnel = matches.first().map(|t| (*t).clone());
        }
        let tunnel = match tunnel {
            Some(t) => t,
            None => self
                .cf(
                    &format!("/accounts/{account}/cfd_tunnel"),
                    Method::POST,
                    Some(json!({"name":name,"config_src":"local","tunnel_secret":tunnel_secret()})),
                )
                .await?["result"]
                .clone(),
        };
        let id = s(&tunnel, "id");
        anyhow::ensure!(!id.is_empty(), "Cloudflare returned no tunnel ID");
        let token = self
            .cf(
                &format!("/accounts/{account}/cfd_tunnel/{id}/token"),
                Method::GET,
                None,
            )
            .await?["result"]
            .clone();
        anyhow::ensure!(
            token.as_str().is_some_and(|s| !s.is_empty()),
            "Cloudflare returned no runner tunnel token"
        );
        let slot = json!({"n":number,"tunnel":id,"token":token});
        self.store.exec(
            "INSERT OR REPLACE INTO slots(n,tunnel,token,created) VALUES(?,?,?,?)",
            &[json!(number), json!(id), token, json!(now)],
        )?;
        self.slots.insert(number, slot.clone());
        self.dns_dirty = true;
        Ok(slot)
    }
    pub async fn join(&mut self, body: &Value, now: u64) -> anyhow::Result<Value> {
        let mut answer = self.reserve_join(body, now)?;
        let slot = self.tunnel_for(n(&answer, "machine"), now).await?;
        answer["tunnelToken"] = slot["token"].clone();
        Ok(answer)
    }
    pub fn public_hosts(&self, name: &str, now: u64) -> BTreeSet<u64> {
        let live = self.live_machines(now);
        let mut copies = self
            .copies_of(name)
            .iter()
            .filter(|x| live.get(&x.machine).is_some_and(|r| r.n("protocol") >= 5))
            .collect::<Vec<_>>();
        let serving = |x: &Copy| {
            live.get(&x.machine).is_some_and(|r| {
                r.v("publicTunnels")[name]
                    == self
                        .public_tunnels
                        .get(name)
                        .map(|t| t["id"].clone())
                        .unwrap_or(Value::Null)
            })
        };
        copies.sort_by_key(|x| (!serving(x), x.leaving.is_some(), x.replica, x.machine));
        let mut hosts = BTreeSet::new();
        for x in copies {
            hosts.insert(x.machine);
            if hosts.len() == 20 {
                break;
            }
        }
        hosts
    }
    pub fn public_tunnels_for(&self, r: &Run, desired: &Value, now: u64) -> Value {
        let mut out = serde_json::Map::new();
        for plan in desired
            .as_object()
            .into_iter()
            .flat_map(|m| m.values())
            .filter(|p| p["port"].as_u64().is_some_and(|p| p > 0))
        {
            let name = s(plan, "app");
            if let Some(t) = self
                .public_tunnels
                .get(name)
                .filter(|t| !s(t, "token").is_empty())
            {
                if self.public_hosts(name, now).contains(&r.machine) {
                    out.insert(name.into(), json!({"id":t["id"],"token":t["token"]}));
                }
            }
        }
        Value::Object(out)
    }
    pub async fn ensure_public_tunnels(&mut self, now: u64) -> anyhow::Result<Vec<String>> {
        let mut errors = Vec::new();
        let projects = self.projects.values().cloned().collect::<Vec<_>>();
        for p in projects {
            if self
                .version(&p.name, p.version)?
                .is_none_or(|s| s.port.is_none())
                || self
                    .public_tunnels
                    .get(&p.name)
                    .is_some_and(|t| !s(t, "token").is_empty())
            {
                continue;
            }
            let result=async{
            let account=format!("/accounts/{}/cfd_tunnel",self.e("ACCOUNT_ID"));let name=self.public_tunnel_name(&p.name);let mut tunnel=self.public_tunnels.get(&p.name).cloned();if tunnel.is_none(){let data=self.cf(&format!("{account}?name={name}&is_deleted=false"),Method::GET,None).await?;let matches=array(&data["result"]).iter().filter(|t|t["name"]==name&&t["deleted_at"].is_null()).collect::<Vec<_>>();anyhow::ensure!(matches.len()<=1,"duplicate shared app tunnel names");let t=if let Some(t)=matches.first(){(*t).clone()}else{self.cf(&account,Method::POST,Some(json!({"name":name,"config_src":"local","tunnel_secret":tunnel_secret()}))).await?["result"].clone()};anyhow::ensure!(!s(&t,"id").is_empty()&&(s(&t,"config_src").is_empty()||t["config_src"]=="local"),"invalid shared app tunnel");let t=json!({"id":t["id"],"token":""});self.public_tunnel_ids.insert(s(&t,"id").into());self.public_tunnels.insert(p.name.clone(),t.clone());tunnel=Some(t);}
            let mut tunnel=tunnel.unwrap();let token=self.cf(&format!("{account}/{}/token",s(&tunnel,"id")),Method::GET,None).await?["result"].clone();anyhow::ensure!(token.as_str().is_some_and(|s|!s.is_empty()),"Cloudflare returned no app tunnel token");tunnel["token"]=token;self.public_tunnels.insert(p.name.clone(),tunnel);for r in self.live_runs(now){if !self.copies_on(&p.name,r.machine).is_empty(){self.wakes.insert(r.id);}}Ok::<_,anyhow::Error>(())
        }.await;
            if let Err(e) = result {
                errors.push(format!(
                    "Shared address for {}: {}",
                    p.name,
                    self.redact(&e.to_string())
                ));
            }
        }
        Ok(errors)
    }
    pub fn dns_targets(&self, now: u64) -> anyhow::Result<BTreeMap<String, String>> {
        let newest = self.live_machines(now);
        let mut want = BTreeMap::new();
        for p in self.projects.values() {
            let Some(spec) = self
                .version(&p.name, p.version)?
                .filter(|s| s.port.is_some())
            else {
                continue;
            };
            if let Some(t) = self.public_tunnels.get(&p.name) {
                want.insert(p.name.clone(), s(t, "id").into());
            }
            for k in 1..=spec.replicas {
                let copies = self
                    .copies_of(&p.name)
                    .iter()
                    .filter(|x| x.replica == k)
                    .collect::<Vec<_>>();
                let healthy = |x: &&Copy| {
                    newest
                        .get(&x.machine)
                        .is_some_and(|r| r.status[&x.key]["s"] == "healthy")
                };
                let pick = copies
                    .iter()
                    .copied()
                    .find(|x| x.leaving.is_none() && healthy(x))
                    .or_else(|| {
                        copies
                            .iter()
                            .copied()
                            .find(|x| x.leaving.is_some() && healthy(x))
                    })
                    .or_else(|| copies.iter().copied().find(|x| x.leaving.is_none()))
                    .or_else(|| copies.first().copied());
                if let Some(slot) = pick.and_then(|x| self.slots.get(&x.machine)) {
                    want.insert(format!("{}-{k}", p.name), s(slot, "tunnel").into());
                }
            }
        }
        Ok(want)
    }
    pub fn follow_copies(&mut self, now: u64) -> anyhow::Result<()> {
        let seen = serde_json::to_string(&self.dns_targets(now)?)?;
        if seen != self.dns_targets_seen {
            self.dns_targets_seen = seen;
            self.dns_dirty = true;
        }
        Ok(())
    }
    pub async fn collect_public_tunnels(
        &mut self,
        records: &[Value],
        now: u64,
    ) -> anyhow::Result<()> {
        let mut needed = BTreeSet::new();
        for p in self.projects.values() {
            if self
                .version(&p.name, p.version)?
                .is_some_and(|s| s.port.is_some())
            {
                if let Some(t) = self.public_tunnels.get(&p.name) {
                    needed.insert(s(t, "id").to_owned());
                }
            }
        }
        self.public_cleanup_due = None;
        for id in self.public_tunnel_ids.clone() {
            if needed.contains(&id)
                || records
                    .iter()
                    .any(|r| r["content"] == format!("{id}.cfargotunnel.com"))
            {
                continue;
            }
            let path = format!("/accounts/{}/cfd_tunnel/{id}", self.e("ACCOUNT_ID"));
            if self.cf(&path, Method::DELETE, None).await.is_ok() {
                self.public_tunnel_ids.remove(&id);
                let names = self
                    .public_tunnels
                    .iter()
                    .filter(|(_, t)| t["id"] == id)
                    .map(|(n, _)| n.clone())
                    .collect::<Vec<_>>();
                for name in names {
                    if let Some(t) = self.public_tunnels.remove(&name) {
                        self.tunnel_secrets.insert(s(&t, "token").into());
                    }
                }
            } else {
                self.public_cleanup_due = Some(now + MIN);
            }
        }
        Ok(())
    }
    pub async fn sync_dns(&mut self, now: u64) -> anyhow::Result<()> {
        if !self.tunnels_ready {
            return Err(http(
                503,
                "recovering Cloudflare tunnels; DNS changes are on hold",
            ));
        }
        let domain = self.e("DOMAIN").to_owned();
        let zone = self.e("ZONE").to_owned();
        let public_errors = self.ensure_public_tunnels(now).await?;
        let shared = self
            .public_tunnel_ids
            .iter()
            .map(|id| format!("{id}.cfargotunnel.com"))
            .collect::<BTreeSet<_>>();
        let mut ours = self
            .slots
            .values()
            .map(|s| format!("{}.cfargotunnel.com", s["tunnel"].as_str().unwrap_or("")))
            .collect::<BTreeSet<_>>();
        ours.extend(shared.iter().cloned());
        let want = self
            .dns_targets(now)?
            .into_iter()
            .map(|(n, id)| (format!("{n}.{domain}"), format!("{id}.cfargotunnel.com")))
            .collect::<BTreeMap<_, _>>();
        let mut names = BTreeSet::new();
        for p in self.projects.values() {
            if let Some(spec) = self
                .version(&p.name, p.version)?
                .filter(|s| s.port.is_some())
            {
                names.insert(format!("{}.{domain}", p.name));
                for k in 1..=spec.replicas {
                    names.insert(format!("{}-{k}.{domain}", p.name));
                }
            }
        }
        let mut existing = Vec::new();
        for page in 1..=1000 {
            let data = self
                .cf(
                    &format!("/zones/{zone}/dns_records?per_page=1000&page={page}"),
                    Method::GET,
                    None,
                )
                .await?;
            existing.extend(array(&data["result"]).iter().cloned());
            if page >= n(&data["result_info"], "total_pages").max(1) {
                break;
            }
            anyhow::ensure!(
                existing.len() < 100_000 && page < 1000,
                "DNS listing exceeds its bound"
            );
        }
        let deletes = existing
            .iter()
            .filter(|r| {
                r["type"] == "CNAME"
                    && ours.contains(s(r, "content"))
                    && !names.contains(s(r, "name"))
                    && (!shared.contains(s(r, "content")) || r["comment"] == "runner fleet")
            })
            .map(|r| json!({"id":r["id"]}))
            .collect::<Vec<_>>();
        let gone = deletes
            .iter()
            .map(|r| s(r, "id").to_owned())
            .collect::<BTreeSet<_>>();
        let mut notes = public_errors.clone();
        let mut posts = Vec::new();
        let mut patches = Vec::new();
        let mut repointed = Vec::new();
        for (name, content) in want {
            let rs = existing
                .iter()
                .filter(|r| r["name"] == name && !gone.contains(s(r, "id")))
                .collect::<Vec<_>>();
            let r = rs.iter().find(|r| r["type"] == "CNAME").copied();
            if rs.is_empty() {
                posts.push(json!({"type":"CNAME","name":name,"content":content,"proxied":true,"comment":"runner fleet"}));
            } else if let Some(r) =
                r.filter(|r| r["content"] != content && ours.contains(s(r, "content")))
            {
                patches.push(
                    json!({"id":r["id"],"content":content,"comment":"runner fleet","proxied":true}),
                );
                repointed.push((name, content));
            } else if let Some(r) = r.filter(|r| {
                r["content"] == content
                    && ours.contains(s(r, "content"))
                    && (r["comment"] != "runner fleet" || r["proxied"] == false)
            }) {
                patches.push(json!({"id":r["id"],"comment":"runner fleet","proxied":true}));
            } else if r.is_none_or(|r| !ours.contains(s(r, "content"))) {
                notes.push(format!(
                    "{name} is already used by another DNS record, so it can't point at its server"
                ));
            }
        }
        let ops = deletes
            .iter()
            .map(|v| ("deletes", v))
            .chain(patches.iter().map(|v| ("patches", v)))
            .chain(posts.iter().map(|v| ("posts", v)))
            .collect::<Vec<_>>();
        for chunk in ops.chunks(100) {
            let mut batch = BTreeMap::<&str, Vec<&Value>>::new();
            for (kind, v) in chunk {
                batch.entry(kind).or_default().push(v);
            }
            self.cf(
                &format!("/zones/{zone}/dns_records/batch"),
                Method::POST,
                Some(json!(batch)),
            )
            .await?;
        }
        for (name, content) in repointed {
            let label = name.strip_suffix(&format!(".{domain}")).unwrap_or(&name);
            if let Some((app, k)) = label
                .rsplit_once('-')
                .and_then(|(app, k)| k.parse::<u32>().ok().map(|k| (app, k)))
            {
                let machine = self
                    .slots
                    .iter()
                    .find(|(_, slot)| format!("{}.cfargotunnel.com", s(slot, "tunnel")) == content)
                    .map(|(m, _)| *m);
                self.log_event("dns", json!({"app":app,"replica":k,"machine":machine}), now)?;
            }
        }
        self.dns_notes = notes;
        let mut remaining = existing
            .into_iter()
            .filter(|r| !gone.contains(s(r, "id")))
            .map(|mut r| {
                if let Some(patch) = patches.iter().find(|p| p["id"] == r["id"]) {
                    r.as_object_mut()
                        .unwrap()
                        .extend(patch.as_object().unwrap().clone());
                }
                r
            })
            .collect::<Vec<_>>();
        remaining.extend(posts);
        self.collect_public_tunnels(&remaining, now).await?;
        if !public_errors.is_empty() {
            self.public_cleanup_due = Some(now + MIN);
        }
        Ok(())
    }
    pub async fn alarm(&mut self, now: u64) -> anyhow::Result<()> {
        if !self.dns_dirty && now < self.dns_due {
            return Ok(());
        }
        self.dns_dirty = false;
        if !self.tunnels_ready {
            if let Err(e) = self.recover_tunnels(now).await {
                let error = self.redact(&e.to_string());
                self.tunnel_recovery_error = Some(error.clone());
                self.dns_error = Some(format!("Tunnel discovery failed: {error}"));
                self.dns_due = now + MIN;
                return Ok(());
            }
        }
        if self.recovery_active(now) {
            self.dns_due = (now + 30_000).max(self.recovering_until);
            return Ok(());
        }
        let result = if self.e("DNS") == "off" {
            self.dns_notes = vec!["DNS sync is switched off (DNS=off)".into()];
            Ok(())
        } else {
            self.sync_dns(now).await
        };
        self.dns_dirty = false;
        match result {
            Ok(()) => {
                self.dns_error = None;
                self.dns_synced_at = now;
                self.dns_due = (now + 60 * MIN).min(self.public_cleanup_due.unwrap_or(u64::MAX));
            }
            Err(e) => {
                self.dns_error = Some(self.redact(&e.to_string()));
                self.dns_due = now + MIN;
            }
        }
        Ok(())
    }
    pub async fn retire_slot(&mut self, number: u64, now: u64) -> anyhow::Result<Value> {
        if self.recovery_active(now) {
            return Err(http(
                503,
                "recovering fleet inventory and tunnels; retry shortly",
            ));
        }
        if number == 0 {
            return Err(http(400, "slot must be a server number"));
        }
        if self.live_runs(now).iter().any(|r| r.machine == number) {
            return Err(http(
                409,
                format!("server {number} is still up; stop it first"),
            ));
        }
        let slot = self.slots.get(&number).cloned();
        if let Some(slot) = &slot {
            let zone = self.e("ZONE").to_owned();
            let records=self.cf(&format!("/zones/{zone}/dns_records?type=CNAME&content={}.cfargotunnel.com&per_page=1000",s(slot,"tunnel")),Method::GET,None).await?;
            if !array(&records["result"]).is_empty() {
                self.cf(&format!("/zones/{zone}/dns_records/batch"),Method::POST,Some(json!({"deletes":array(&records["result"]).iter().map(|r|json!({"id":r["id"]})).collect::<Vec<_>>()}))).await?;
            }
            self.cf(
                &format!(
                    "/accounts/{}/cfd_tunnel/{}?cascade=true",
                    self.e("ACCOUNT_ID"),
                    s(slot, "tunnel")
                ),
                Method::DELETE,
                None,
            )
            .await?;
            self.slots.remove(&number);
            self.store
                .exec("DELETE FROM slots WHERE n=?", &[json!(number)])?;
        }
        self.agents.retain(|_, a| a["slot"] != number);
        self.store
            .exec("DELETE FROM agents WHERE slot=?", &[json!(number)])?;
        let ids = self
            .runs
            .values()
            .filter(|r| r.machine == number)
            .map(|r| r.id.clone())
            .collect::<Vec<_>>();
        for id in ids {
            self.runs.remove(&id);
            self.observed_runs.remove(&id);
        }
        self.store
            .exec("DELETE FROM runs WHERE machine=?", &[json!(number)])?;
        self.starts.remove(&number);
        self.store
            .exec("DELETE FROM starts WHERE machine=?", &[json!(number)])?;
        self.holds.remove(&number);
        self.live_metrics.remove(&number);
        for (name, list) in self.copies.clone() {
            for x in list.iter().filter(|x| x.machine == number) {
                self.drop_copy(&name, number, x.replica, None, now)?;
            }
        }
        self.dns_dirty = true;
        Ok(json!({"retired":number,"tunnel":slot.as_ref().map(|s|&s["tunnel"])}))
    }
}
