const get = async (url) => {
  const response = await fetch(url, { signal: AbortSignal.timeout(3500) });
  if (!response.ok) throw new Error("service is not ready");
  return response;
};
try {
  if (Number(process.env.CP_REPLICAS || 1) > 1 && process.env.CP_WARM_REPLACEMENT === '1') {
    // Healthy replacement, waiting for its predecessor to release the vote.
    await get('http://127.0.0.1:' + (process.env.PORT || 8920) + '/readyz');
  } else {
  const status = await (await get(`http://127.0.0.1:${process.env.PORT || 8920}/healthz`)).json();
  if (!status.tunnelsReady) throw new Error("tunnel inventory is not ready");
  if (!["0", "false"].includes(process.env.AUTO_TUNNEL)) await get(`http://127.0.0.1:${process.env.TUNNEL_METRICS_PORT || 49312}/ready`);
  }
} catch { process.exitCode = 1; }
