export interface Env {
  WEBHOOK_SECRET: string;
  SERVER1_URL: string;  // e.g. https://104.239.66.110
  SERVER2_URL: string;  // e.g. https://72.61.90.52
}

interface CapacityResponse {
  available: number;
  active: number;
  max: number;
}

async function verifySignature(body: ArrayBuffer, sigHeader: string, secret: string): Promise<boolean> {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw", enc.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, body);
  const hex = Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, "0")).join("");
  const expected = "sha256=" + hex;
  if (expected.length !== sigHeader.length) return false;
  // Constant-time comparison
  let diff = 0;
  for (let i = 0; i < expected.length; i++) diff |= expected.charCodeAt(i) ^ sigHeader.charCodeAt(i);
  return diff === 0;
}

async function getCapacity(serverUrl: string): Promise<number> {
  try {
    const resp = await fetch(`${serverUrl}/capacity`, {
      signal: AbortSignal.timeout(3000),
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      cf: { disableTLSVerification: true } as any,
    });
    if (!resp.ok) return 0;
    const data = await resp.json() as CapacityResponse;
    return data.available ?? 0;
  } catch {
    return 0;
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (request.method !== "POST" || url.pathname !== "/webhook") {
      return new Response("Not found", { status: 404 });
    }

    const body = await request.arrayBuffer();
    const sig = request.headers.get("X-Hub-Signature-256") ?? "";

    if (!(await verifySignature(body, sig, env.WEBHOOK_SECRET))) {
      return new Response("Unauthorized", { status: 401 });
    }

    // Pick least-loaded server (most available slots wins).
    const [cap1, cap2] = await Promise.all([
      getCapacity(env.SERVER1_URL),
      getCapacity(env.SERVER2_URL),
    ]);

    const target = cap1 >= cap2 ? env.SERVER1_URL : env.SERVER2_URL;

    const forwardHeaders = new Headers(request.headers);
    forwardHeaders.set("X-Forwarded-For", request.headers.get("CF-Connecting-IP") ?? "");

    const upstream = await fetch(`${target}/webhook`, {
      method: "POST",
      headers: forwardHeaders,
      body,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      cf: { disableTLSVerification: true } as any,
    });

    return new Response(upstream.body, {
      status: upstream.status,
      headers: { "Content-Type": "application/json" },
    });
  },
};
