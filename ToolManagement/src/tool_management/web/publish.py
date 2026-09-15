"""Standalone marketing and consent pages for Distributed Publish."""

from __future__ import annotations

from html import escape
from typing import Dict


def publish_page_headers(nonce: str) -> Dict[str, str]:
    return {
        "Cache-Control": "no-store, private",
        "Content-Security-Policy": (
            "default-src 'none'; "
            f"style-src 'nonce-{nonce}'; script-src 'nonce-{nonce}'; "
            "connect-src 'self'; img-src 'self' data:; font-src 'self'; "
            "base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
        ),
        "Referrer-Policy": "no-referrer",
        "Permissions-Policy": "camera=(), microphone=(), geolocation=(), payment=()",
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
    }


_BASE_STYLE = r"""
  :root {
    --ink:#12130f; --paper:#f3efe5; --paper-2:#e8e0d0; --red:#f05238;
    --lime:#c5f45d; --blue:#9bc9ff; --muted:#696b61; --line:#171812;
    --display: "Arial Narrow", "Helvetica Neue", Arial, sans-serif;
    --body: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  }
  * { box-sizing:border-box; }
  [hidden] { display:none !important; }
  html { scroll-behavior:smooth; background:var(--ink); }
  body { margin:0; background:var(--paper); color:var(--ink); font-family:var(--body); }
  a { color:inherit; }
  .shell { width:min(1180px, calc(100% - 40px)); margin:0 auto; }
  .nav { height:76px; display:flex; align-items:center; justify-content:space-between; border-bottom:2px solid var(--line); }
  .brand { display:flex; gap:11px; align-items:center; text-decoration:none; font:900 14px/1 var(--display); text-transform:uppercase; letter-spacing:.06em; }
  .mark { width:35px; height:35px; display:grid; place-items:center; border:2px solid var(--line); background:var(--lime); box-shadow:4px 4px 0 var(--line); }
  .navlinks { display:flex; gap:24px; align-items:center; font-size:13px; font-weight:750; }
  .navlinks a { text-decoration:none; }
  .button { appearance:none; border:2px solid var(--line); padding:13px 18px; background:var(--ink); color:#fff; text-decoration:none; font:850 13px/1 var(--body); cursor:pointer; box-shadow:5px 5px 0 var(--red); transition:.15s ease; }
  .button:hover { transform:translate(-2px,-2px); box-shadow:7px 7px 0 var(--red); }
  .button.light { background:var(--paper); color:var(--ink); box-shadow:5px 5px 0 var(--lime); }
  .eyebrow { display:inline-flex; padding:7px 10px; border:1.5px solid var(--line); background:var(--lime); font:850 11px/1 var(--body); text-transform:uppercase; letter-spacing:.11em; }
  .mono { font-family:ui-monospace, SFMono-Regular, Menlo, monospace; }
  footer { background:var(--ink); color:#eae7df; padding:34px 0; }
  footer .shell { display:flex; justify-content:space-between; gap:24px; align-items:center; font-size:12px; }
  @media (max-width:720px) {
    .shell { width:min(100% - 26px, 1180px); }
    .navlinks a:not(.button) { display:none; }
    footer .shell { align-items:flex-start; flex-direction:column; }
  }
"""


def render_publish_marketing(nonce: str) -> str:
    return r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="description" content="Distributed Publish turns research into reviewable Substack drafts, then waits for a human to publish.">
<title>Distributed Publish — Keep a human on the send button</title>
<style nonce="__NONCE__">__BASE_STYLE__
  .hero { display:grid; grid-template-columns:1.15fr .85fr; min-height:660px; border-bottom:2px solid var(--line); }
  .hero-copy { padding:86px 64px 72px 0; border-right:2px solid var(--line); }
  h1 { max-width:790px; margin:25px 0 24px; font:950 clamp(54px,7.2vw,105px)/.86 var(--display); letter-spacing:-.065em; text-transform:uppercase; }
  h1 em { display:block; color:var(--red); font-style:normal; }
  .lede { max-width:650px; font-size:19px; line-height:1.5; color:#34362f; }
  .hero-actions { display:flex; flex-wrap:wrap; gap:16px; align-items:center; margin-top:34px; }
  .proof { font-size:12px; color:var(--muted); max-width:210px; line-height:1.4; }
  .hero-panel { padding:70px 0 60px 48px; display:flex; align-items:center; }
  .post { width:100%; border:2px solid var(--line); background:#fff; box-shadow:10px 10px 0 var(--line); }
  .post-top { padding:16px 18px; display:flex; align-items:center; justify-content:space-between; border-bottom:2px solid var(--line); }
  .avatar { width:34px; height:34px; border:2px solid var(--line); border-radius:50%; background:var(--blue); }
  .post-id { display:flex; gap:10px; align-items:center; font-size:12px; font-weight:800; }
  .post-state { background:#ffdfd8; color:#8b2717; padding:6px 8px; font:800 10px/1 var(--body); text-transform:uppercase; letter-spacing:.09em; }
  .post-body { padding:23px 22px 10px; }
  .post-body h3 { margin:0 0 14px; font:850 29px/1.02 var(--display); text-transform:uppercase; }
  .post-body p { font-size:14px; line-height:1.55; }
  .audit { display:grid; grid-template-columns:30px 1fr; gap:10px 12px; padding:12px 22px 22px; font-size:12px; }
  .check { width:24px; height:24px; border:1.5px solid var(--line); display:grid; place-items:center; background:var(--lime); font-weight:950; }
  .pending { background:var(--paper-2); }
  .reaction { margin:0 22px 22px; padding:13px 14px; border:1.5px solid var(--line); background:#fff1bd; font-size:12px; line-height:1.45; transform:rotate(-1deg); }
  .manifesto { background:var(--ink); color:#f2eee4; }
  .manifesto .shell { display:grid; grid-template-columns:.9fr 1.1fr; gap:80px; padding:95px 0; }
  .manifesto h2, .section-head h2 { margin:18px 0; font:930 clamp(42px,5vw,72px)/.92 var(--display); letter-spacing:-.045em; text-transform:uppercase; }
  .manifesto blockquote { margin:0; font:500 clamp(24px,3vw,42px)/1.14 Georgia, serif; color:#f4b5a9; }
  .manifesto p { color:#b9bbb2; line-height:1.65; }
  .how { padding:100px 0; border-bottom:2px solid var(--line); }
  .section-head { display:flex; justify-content:space-between; align-items:end; gap:30px; }
  .section-head p { max-width:430px; color:var(--muted); line-height:1.6; }
  .steps { display:grid; grid-template-columns:repeat(4,1fr); margin-top:48px; border:2px solid var(--line); }
  .step { min-height:245px; padding:23px; border-right:2px solid var(--line); position:relative; }
  .step:last-child { border-right:0; }
  .step:nth-child(2) { background:var(--blue); }
  .step:nth-child(3) { background:var(--lime); }
  .step-num { font:900 12px/1 var(--body); }
  .step h3 { margin:56px 0 12px; font:900 25px/1 var(--display); text-transform:uppercase; }
  .step p { color:#40423b; font-size:13px; line-height:1.5; }
  .consent { padding:105px 0; background:#e6ded0; }
  .consent-grid { display:grid; grid-template-columns:.88fr 1.12fr; gap:70px; align-items:start; }
  .consent h2 { margin:18px 0; font:930 clamp(46px,5.5vw,78px)/.9 var(--display); text-transform:uppercase; letter-spacing:-.05em; }
  .consent-copy > p { color:#55574f; line-height:1.65; max-width:510px; }
  .guardrails { margin-top:30px; display:grid; gap:12px; }
  .guard { display:flex; gap:12px; align-items:flex-start; font-size:13px; }
  .guard b { display:block; margin-bottom:3px; }
  .form-card { background:var(--paper); border:2px solid var(--line); padding:27px; box-shadow:10px 10px 0 var(--red); }
  .form-card h3 { margin:0 0 6px; font:900 27px/1 var(--display); text-transform:uppercase; }
  .form-card > p { margin:0 0 23px; color:var(--muted); font-size:13px; line-height:1.5; }
  label { display:block; margin:16px 0 7px; font-size:11px; font-weight:850; text-transform:uppercase; letter-spacing:.08em; }
  input { width:100%; border:1.5px solid var(--line); border-radius:0; background:#fff; padding:13px; color:var(--ink); font:14px var(--body); }
  input:focus { outline:3px solid var(--blue); outline-offset:1px; }
  .hint { color:var(--muted); font-size:11px; line-height:1.45; margin-top:7px; }
  .form-card .button { width:100%; margin-top:21px; padding:15px; }
  #connectStatus { min-height:22px; margin-top:15px; font-size:12px; line-height:1.45; }
  #connectStatus.ok { color:#236a26; } #connectStatus.err { color:#a02b1c; }
  .secret-box { margin-top:14px; padding:12px; border:1.5px dashed var(--line); background:#fff7d7; overflow-wrap:anywhere; }
  .secret-box code { font-size:11px; }
  .secret-box a { display:inline-block; margin-top:10px; font-weight:850; }
  @media (max-width:900px) {
    .hero, .manifesto .shell, .consent-grid { grid-template-columns:1fr; }
    .hero-copy { border-right:0; padding-right:0; }
    .hero-panel { padding:45px 10px 65px 0; }
    .steps { grid-template-columns:1fr 1fr; }
    .step:nth-child(2) { border-right:0; }
    .step:nth-child(-n+2) { border-bottom:2px solid var(--line); }
  }
  @media (max-width:560px) {
    .hero-copy { padding-top:58px; }
    h1 { font-size:55px; }
    .steps { grid-template-columns:1fr; }
    .step { border-right:0; border-bottom:2px solid var(--line); min-height:205px; }
    .step:last-child { border-bottom:0; }
    .section-head { display:block; }
  }
</style>
</head>
<body>
<header class="shell nav">
  <a class="brand" href="/"><span class="mark">D/S</span><span>Distributed Publish</span></a>
  <nav class="navlinks" aria-label="Primary">
    <a href="#principles">Principles</a><a href="#workflow">Workflow</a><a href="/account">Account</a>
    <a class="button" href="#connect">Connect Substack</a>
  </nav>
</header>

<main>
  <section class="shell hero">
    <div class="hero-copy">
      <span class="eyebrow">Autonomous production. Accountable publishing.</span>
      <h1>Your voice is not <em>a background job.</em></h1>
      <p class="lede">Research, structure, draft, illustrate, and syndicate with agents. Then put a real person back at the moment that matters: authorization.</p>
      <div class="hero-actions">
        <a class="button" href="#connect">Connect a publication</a>
        <a class="button light" href="#workflow">See the control loop</a>
        <span class="proof">Nothing is posted or emailed without an explicit approval signal.</span>
      </div>
    </div>
    <div class="hero-panel" aria-label="Draft-first publishing preview">
      <article class="post">
        <div class="post-top"><div class="post-id"><span class="avatar"></span><span>YOUR PUBLICATION<br><small>Agent-assisted</small></span></div><span class="post-state">Draft — not public</span></div>
        <div class="post-body"><h3>The work moves fast. The judgment stays yours.</h3><p>The agent assembled sources, preserved links, and formatted native Substack blocks. It did not impersonate a decision.</p></div>
        <div class="audit">
          <span class="check">✓</span><span><b>Sources attached</b><br>Claims remain inspectable.</span>
          <span class="check">✓</span><span><b>Draft saved</b><br>No subscribers notified.</span>
          <span class="check pending">…</span><span><b>Human authorization pending</b><br>Publish remains locked.</span>
        </div>
        <div class="reaction"><b>The audience test:</b> Can the author explain what the system did, inspect it, and own the final call? If not, it is not ready to send.</div>
      </article>
    </div>
  </section>

  <section class="manifesto" id="principles">
    <div class="shell">
      <div><span class="eyebrow">The line we hold</span><h2>Assistance without abdication.</h2><p>People do not need a synthetic byline. They need leverage that keeps provenance, judgment, and responsibility visible.</p></div>
      <blockquote>“Automate the production burden. Never automate away the author.”</blockquote>
    </div>
  </section>

  <section class="how shell" id="workflow">
    <div class="section-head"><div><span class="eyebrow">A governed control loop</span><h2>Fast where it should be.<br>Slow where it matters.</h2></div><p>Distributed Publish is a core integration server for native Substack drafts, media, notes, webhooks, event delivery, and controlled syndication.</p></div>
    <div class="steps">
      <article class="step"><span class="step-num">01 / ASSEMBLE</span><h3>Bring the evidence</h3><p>Agents gather research, transcripts, links, and owned media into one traceable working set.</p></article>
      <article class="step"><span class="step-num">02 / DRAFT</span><h3>Build natively</h3><p>Create real Substack document blocks and a reviewable draft—not a flattened paste or a public post.</p></article>
      <article class="step"><span class="step-num">03 / AUTHORIZE</span><h3>Ask the human</h3><p>An expiring consent link requests the session connection. A signed webhook reports requested, authorized, or cancelled state.</p></article>
      <article class="step"><span class="step-num">04 / PUBLISH</span><h3>Confirm the send</h3><p>Publishing and subscriber email are explicit, separate confirmations with auditable intent.</p></article>
    </div>
  </section>

  <section class="consent" id="connect">
    <div class="shell consent-grid">
      <div class="consent-copy">
        <span class="eyebrow">Consent-first connection</span>
        <h2>Ask for access. Never take it.</h2>
        <p>A Substack SID represents an active signed-in session. Distributed Publish does not scrape it from a browser. It creates a one-time authorization link where the account owner can provide it deliberately.</p>
        <div class="guardrails">
          <div class="guard"><span class="check">1</span><span><b>Short-lived request</b>The link expires in 15 minutes and can be used once.</span></div>
          <div class="guard"><span class="check">2</span><span><b>Fragment-held token</b>The secret is never sent in page requests or referrer headers.</span></div>
          <div class="guard"><span class="check">3</span><span><b>Encrypted tenant vault</b>The SID never appears in connection status or webhook payloads.</span></div>
          <div class="guard"><span class="check">4</span><span><b>Revocable</b>Disconnecting removes the stored session and cancels pending requests.</span></div>
        </div>
      </div>
      <div class="form-card">
        <h3>Create an authorization request</h3>
        <p>Your DSCO account must be signed in in this tab. A callback is optional; when supplied, it receives a signed event carrying the consent link so your own workflow can ask the user.</p>
        <form id="connectForm">
          <label for="publication">Publication hostname</label>
          <input id="publication" name="publication" autocomplete="url" placeholder="your-publication.substack.com" required>
          <div class="hint">A Substack hostname or configured custom publication domain.</div>
          <label for="callback">Notification webhook <span aria-hidden="true">·</span> optional</label>
          <input id="callback" name="callback" type="url" autocomplete="off" placeholder="https://your-app.example/webhooks/publish">
          <div class="hint">Public HTTPS only. Redirects are not followed. Delivery is signed and retried.</div>
          <button class="button" type="submit">Create consent link →</button>
        </form>
        <div id="connectStatus" role="status" aria-live="polite"></div>
        <div id="secretBox" class="secret-box" hidden><b>Copy this webhook secret now</b><br><code id="webhookSecret"></code><br><a id="authorizationLink" href="#">Continue to authorization →</a></div>
      </div>
    </div>
  </section>
</main>

<footer><div class="shell"><span><b>DISTRIBUTED PUBLISH</b> · A core DSCO integration server</span><span>Draft-first · consent-first · human-authorized</span></div></footer>
<script nonce="__NONCE__">
(function(){
  const form=document.getElementById('connectForm');
  const statusEl=document.getElementById('connectStatus');
  const secretBox=document.getElementById('secretBox');
  function message(text,ok){ statusEl.textContent=text; statusEl.className=ok?'ok':'err'; }
  form.addEventListener('submit',async function(event){
    event.preventDefault(); secretBox.hidden=true;
    const key=sessionStorage.getItem('dsco_user_key')||'';
    if(!key){ message('Sign in at /account in this tab, then return here to create the request.',false); return; }
    const publication=document.getElementById('publication').value.trim();
    const callback=document.getElementById('callback').value.trim();
    const payload={publication:publication};
    if(callback) payload.notification_webhook_url=callback;
    message('Creating an expiring consent request…',true);
    try {
      const response=await fetch('/api/v1/publish/authorizations',{
        method:'POST', headers:{'content-type':'application/json','authorization':'Bearer '+key},
        body:JSON.stringify(payload)
      });
      const data=await response.json();
      if(!response.ok){ const detail=data.detail||data; throw new Error(typeof detail==='string'?detail:JSON.stringify(detail)); }
      if(data.webhook_secret){
        document.getElementById('webhookSecret').textContent=data.webhook_secret;
        document.getElementById('authorizationLink').href=data.authorization_url;
        secretBox.hidden=false;
        message('Webhook queued. Store its signing secret before continuing—it is shown only in this response.',true);
      } else {
        location.assign(data.authorization_url);
      }
    } catch(error){ message(error.message||'Could not create the authorization request.',false); }
  });
})();
</script>
</body>
</html>""".replace("__NONCE__", escape(nonce, quote=True)).replace("__BASE_STYLE__", _BASE_STYLE)


def render_publish_authorization(request_id: str, nonce: str) -> str:
    safe_request_id = escape(request_id, quote=True)
    return r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow,noarchive">
<title>Authorize Substack — Distributed Publish</title>
<style nonce="__NONCE__">__BASE_STYLE__
  body { min-height:100vh; display:grid; grid-template-rows:auto 1fr auto; }
  .auth-main { width:min(1040px, calc(100% - 36px)); margin:0 auto; padding:58px 0 80px; display:grid; grid-template-columns:.9fr 1.1fr; gap:70px; align-items:start; }
  .auth-copy h1 { margin:18px 0 20px; font:950 clamp(48px,6.5vw,82px)/.88 var(--display); letter-spacing:-.055em; text-transform:uppercase; }
  .auth-copy p { color:#4e5048; line-height:1.65; }
  .warning { margin-top:26px; padding:15px; border:2px solid var(--line); background:#fff2b9; font-size:12px; line-height:1.5; box-shadow:6px 6px 0 var(--line); }
  .auth-card { background:#fff; border:2px solid var(--line); padding:28px; box-shadow:10px 10px 0 var(--red); }
  .auth-card h2 { margin:0 0 5px; font:900 29px/1 var(--display); text-transform:uppercase; }
  .request-id { color:var(--muted); font-size:10px; overflow-wrap:anywhere; }
  .instructions { margin:23px 0; padding:0; counter-reset:item; list-style:none; }
  .instructions li { counter-increment:item; display:grid; grid-template-columns:28px 1fr; gap:10px; margin:12px 0; font-size:13px; line-height:1.45; }
  .instructions li:before { content:counter(item); width:24px; height:24px; border:1.5px solid var(--line); display:grid; place-items:center; background:var(--lime); font-weight:900; }
  label { display:block; margin:15px 0 7px; font-size:11px; font-weight:850; text-transform:uppercase; letter-spacing:.08em; }
  input[type=password] { width:100%; border:1.5px solid var(--line); padding:13px; border-radius:0; font:14px ui-monospace,monospace; }
  input:focus { outline:3px solid var(--blue); outline-offset:1px; }
  .consent-check { display:grid; grid-template-columns:20px 1fr; gap:9px; margin:17px 0; font-size:12px; line-height:1.45; text-transform:none; letter-spacing:normal; font-weight:400; }
  .consent-check input { width:17px; height:17px; accent-color:var(--ink); }
  .auth-card .button { width:100%; margin-top:4px; }
  #authStatus { min-height:20px; margin-top:14px; font-size:12px; }
  #authStatus.ok { color:#236a26; } #authStatus.err { color:#a02b1c; }
  .secure-label { font-size:10px; }
  .done { padding:30px 0 12px; text-align:center; }
  .done .check { width:58px; height:58px; margin:0 auto 18px; font-size:28px; }
  @media(max-width:760px){ .auth-main { grid-template-columns:1fr; gap:36px; padding-top:42px; } }
</style>
</head>
<body>
<header class="shell nav"><a class="brand" href="/publish"><span class="mark">D/S</span><span>Distributed Publish</span></a><span class="mono secure-label">SECURE CONNECTION</span></header>
<main class="auth-main">
  <section class="auth-copy">
    <span class="eyebrow">One-time authorization</span>
    <h1>Connect the session you own.</h1>
    <p>This request lets Distributed Publish create and manage drafts for one publication. Connecting does not publish a post, send subscriber email, or grant blanket approval for future actions.</p>
    <div class="warning"><b>Treat your SID like a password.</b> Anyone holding it may be able to act as your signed-in Substack session. Only continue if you recognize this request and the address bar belongs to your DSCO deployment.</div>
  </section>
  <section class="auth-card">
    <div id="formView">
      <h2>Provide the Substack SID</h2>
      <div class="request-id mono">Request __REQUEST_ID__</div>
      <ol class="instructions">
        <li><span>Sign in to Substack in this browser and open your publication.</span></li>
        <li><span>Open browser developer tools, then Application or Storage → Cookies.</span></li>
        <li><span>Copy only the value of the <span class="mono">substack.sid</span> cookie and paste it below.</span></li>
      </ol>
      <form id="authForm">
        <label for="sid">Substack SID</label>
        <input id="sid" type="password" autocomplete="off" autocapitalize="none" spellcheck="false" required minlength="16" placeholder="Paste the cookie value">
        <label class="consent-check"><input id="consent" type="checkbox" required><span>I own or administer this publication and authorize DSCO to store this session credential for draft-first Substack operations. Publishing still requires a separate explicit confirmation.</span></label>
        <button class="button" type="submit">Authorize connection →</button>
      </form>
      <div id="authStatus" role="status" aria-live="polite"></div>
    </div>
    <div id="doneView" class="done" hidden><span class="check">✓</span><h2>Substack connected</h2><p>The SID is encrypted in your tenant vault. It was not sent to the notification webhook.</p><a href="/publish">Return to Distributed Publish</a></div>
  </section>
</main>
<footer><div class="shell"><span>Consent is scoped. Credentials are revocable.</span><span>No post was published by this action.</span></div></footer>
<script nonce="__NONCE__">
(function(){
  const requestId='__REQUEST_ID__';
  const params=new URLSearchParams(location.hash.slice(1));
  const token=params.get('token')||'';
  history.replaceState(null,'',location.pathname);
  const form=document.getElementById('authForm');
  const statusEl=document.getElementById('authStatus');
  function message(text,ok){ statusEl.textContent=text; statusEl.className=ok?'ok':'err'; }
  if(!token){ form.querySelector('button').disabled=true; message('This authorization link is incomplete. Ask for a new consent request.',false); }
  form.addEventListener('submit',async function(event){
    event.preventDefault();
    if(!token){ return; }
    const sid=document.getElementById('sid').value.trim();
    if(!document.getElementById('consent').checked){ message('Explicit authorization is required.',false); return; }
    message('Encrypting the connection…',true);
    try {
      const response=await fetch('/api/v1/publish/authorizations/'+encodeURIComponent(requestId)+'/complete',{
        method:'POST', headers:{'content-type':'application/json'},
        body:JSON.stringify({authorization_token:token,sid:sid,authorize_connection:true})
      });
      document.getElementById('sid').value='';
      const data=await response.json();
      if(!response.ok){ const detail=data.detail||data; throw new Error(typeof detail==='string'?detail:(detail.error||JSON.stringify(detail))); }
      document.getElementById('formView').hidden=true;
      document.getElementById('doneView').hidden=false;
    } catch(error){ message(error.message||'Authorization failed. Request a fresh link and try again.',false); }
  });
})();
</script>
</body>
</html>""".replace("__NONCE__", escape(nonce, quote=True)).replace("__BASE_STYLE__", _BASE_STYLE).replace("__REQUEST_ID__", safe_request_id)
