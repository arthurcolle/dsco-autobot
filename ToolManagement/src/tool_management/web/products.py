"""Public product-family pages for Search, Research, and Tools.

The pages deliberately share one small, dependency-free visual system.  Keeping
them server-rendered makes them deploy with the Tool Management API without a
second frontend build or CDN.
"""

from __future__ import annotations

from dataclasses import dataclass
from html import escape

from .publish import publish_page_headers


PRODUCT_PATHS = ("/search", "/research", "/publish", "/tools")


@dataclass(frozen=True)
class ProductPage:
    slug: str
    name: str
    eyebrow: str
    title: str
    title_accent: str
    lede: str
    primary_label: str
    primary_href: str
    secondary_label: str
    secondary_href: str
    accent: str
    accent_soft: str
    panel: str
    metrics: tuple[tuple[str, str, str], ...]
    terminal_label: str
    terminal_html: str
    declaration: str
    declaration_detail: str
    steps: tuple[tuple[str, str, str], ...]
    features: tuple[tuple[str, str, str], ...]
    final_title: str
    final_copy: str


_STYLE = r"""
  :root {
    --ink:#11120f; --paper:#f3efe5; --paper-2:#e5dece; --muted:#62655d;
    --line:#151611; --accent:__ACCENT__; --accent-soft:__ACCENT_SOFT__;
    --panel:__PANEL__; --white:#fffdf7;
    --display:"Arial Narrow","Helvetica Neue",Arial,sans-serif;
    --body:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    --mono:ui-monospace,SFMono-Regular,Menlo,Monaco,monospace;
  }
  *{box-sizing:border-box} html{scroll-behavior:smooth;background:var(--ink)}
  body{margin:0;background:var(--paper);color:var(--ink);font-family:var(--body)}
  a{color:inherit}.shell{width:min(1200px,calc(100% - 42px));margin:0 auto}
  .nav{height:76px;display:flex;align-items:center;justify-content:space-between;border-bottom:2px solid var(--line)}
  .brand{display:flex;gap:11px;align-items:center;text-decoration:none;font:900 14px/1 var(--display);text-transform:uppercase;letter-spacing:.055em;white-space:nowrap}
  .mark{width:36px;height:36px;display:grid;place-items:center;border:2px solid var(--line);background:var(--accent);box-shadow:4px 4px 0 var(--line)}
  .navlinks{display:flex;gap:20px;align-items:center;font-size:12px;font-weight:800}
  .navlinks a{text-decoration:none}.navlinks a[aria-current="page"]{border-bottom:2px solid var(--accent);padding-bottom:4px}
  .button{appearance:none;border:2px solid var(--line);padding:13px 18px;background:var(--ink);color:#fff;text-decoration:none;font:850 13px/1 var(--body);cursor:pointer;box-shadow:5px 5px 0 var(--accent);transition:.15s ease;display:inline-flex;align-items:center;justify-content:center}
  .button:hover{transform:translate(-2px,-2px);box-shadow:7px 7px 0 var(--accent)}
  .button.light{background:var(--paper);color:var(--ink);box-shadow:5px 5px 0 var(--panel)}
  .eyebrow{display:inline-flex;padding:7px 10px;border:1.5px solid var(--line);background:var(--accent);font:850 11px/1 var(--body);text-transform:uppercase;letter-spacing:.105em}
  .hero{display:grid;grid-template-columns:1.08fr .92fr;min-height:650px;border-bottom:2px solid var(--line)}
  .hero-copy{padding:84px 62px 72px 0;border-right:2px solid var(--line)}
  h1{max-width:790px;margin:25px 0 24px;font:950 clamp(56px,7vw,104px)/.86 var(--display);letter-spacing:-.064em;text-transform:uppercase}
  h1 em{display:block;color:var(--accent);font-style:normal;-webkit-text-stroke:1.5px var(--line);text-shadow:3px 3px 0 var(--line)}
  .lede{max-width:650px;font-size:19px;line-height:1.5;color:#34362f}
  .actions{display:flex;flex-wrap:wrap;gap:16px;align-items:center;margin-top:34px}
  .proof{font:11px/1.45 var(--mono);color:var(--muted);max-width:220px}
  .hero-panel{padding:66px 0 58px 48px;display:flex;align-items:center}
  .terminal{width:100%;border:2px solid var(--line);background:var(--ink);color:#f6f2e8;box-shadow:11px 11px 0 var(--accent)}
  .terminal-top{height:49px;padding:0 17px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #40423b;font:10px/1 var(--mono);letter-spacing:.09em;text-transform:uppercase;color:#a9aca1}
  .lights{display:flex;gap:6px}.lights i{width:8px;height:8px;border:1px solid #797c72;border-radius:50%}.lights i:first-child{background:var(--accent);border-color:var(--accent)}
  .terminal-body{padding:24px;font:12px/1.65 var(--mono);min-height:390px}.prompt{color:var(--accent)}.dim{color:#777b70}.ok{color:#bdf36c}.hot{color:var(--accent)}
  .call{border:1px solid #3c3f37;padding:12px 13px;margin:13px 0;background:#181a16}.call b{color:var(--accent);font-weight:600}.result{border-left:3px solid var(--accent);padding-left:12px;margin:17px 0;color:#d7dacd}
  .metrics{display:grid;grid-template-columns:repeat(4,1fr);border-bottom:2px solid var(--line)}
  .metric{padding:29px 25px;border-right:2px solid var(--line);min-height:138px}.metric:last-child{border-right:0}.metric:nth-child(2){background:var(--accent-soft)}.metric:nth-child(3){background:var(--accent)}
  .metric .value{font:950 clamp(32px,4vw,55px)/1 var(--display);letter-spacing:-.04em}.metric .label{margin-top:10px;font:850 10px/1.3 var(--body);text-transform:uppercase;letter-spacing:.08em}.metric .note{margin-top:8px;color:var(--muted);font:10px/1.35 var(--mono)}
  .declaration{background:var(--ink);color:#f4f0e6}.declaration .shell{display:grid;grid-template-columns:1.2fr .8fr;gap:80px;padding:95px 0;align-items:end}
  .declaration h2{margin:18px 0 0;font:930 clamp(45px,6vw,82px)/.9 var(--display);letter-spacing:-.05em;text-transform:uppercase}
  .declaration p{color:#b8bbb1;line-height:1.7;margin:0}.declaration p b{color:var(--accent)}
  .section{padding:100px 0;border-bottom:2px solid var(--line)}
  .section-head{display:flex;justify-content:space-between;align-items:end;gap:36px}.section-head h2{margin:18px 0 0;font:930 clamp(43px,5vw,72px)/.91 var(--display);letter-spacing:-.048em;text-transform:uppercase}.section-head p{max-width:430px;color:var(--muted);line-height:1.6}
  .steps{display:grid;grid-template-columns:repeat(4,1fr);margin-top:48px;border:2px solid var(--line)}
  .step{min-height:250px;padding:23px;border-right:2px solid var(--line);position:relative}.step:last-child{border-right:0}.step:nth-child(2){background:var(--panel)}.step:nth-child(3){background:var(--accent)}
  .step-num{font:900 11px/1 var(--mono)}.step h3{margin:58px 0 12px;font:900 25px/1 var(--display);text-transform:uppercase}.step p{color:#40423b;font-size:13px;line-height:1.5}
  .feature-grid{display:grid;grid-template-columns:repeat(3,1fr);margin-top:48px;border-top:2px solid var(--line);border-left:2px solid var(--line)}
  .feature{min-height:240px;padding:26px;border-right:2px solid var(--line);border-bottom:2px solid var(--line);background:var(--white)}.feature:nth-child(2),.feature:nth-child(5){background:var(--paper-2)}
  .feature .tag{font:850 10px/1 var(--mono);text-transform:uppercase;letter-spacing:.08em;color:var(--muted)}.feature h3{margin:44px 0 13px;font:900 27px/1 var(--display);text-transform:uppercase}.feature p{font-size:13px;line-height:1.55;color:#4d5047}
  .topology{margin-top:48px;border:2px solid var(--line);background:var(--ink);color:#f4f1e8;padding:20px;box-shadow:10px 10px 0 var(--accent)}
  .topology-row{display:grid;grid-template-columns:150px 1fr;gap:15px;padding:14px 0;border-bottom:1px solid #3b3d37;align-items:start}.topology-row:last-child{border-bottom:0}.topology-label{font:850 10px/1.4 var(--mono);color:var(--accent);text-transform:uppercase;letter-spacing:.09em}.chips{display:flex;flex-wrap:wrap;gap:7px}.chip{border:1px solid #51544b;padding:6px 8px;font:10px/1 var(--mono);color:#d9dccf;background:#1b1d19}
  .catalog{padding:100px 0;background:var(--paper-2);border-bottom:2px solid var(--line)}.catalog-grid{display:grid;grid-template-columns:.75fr 1.25fr;gap:54px;align-items:start}.catalog h2{margin:18px 0;font:930 clamp(43px,5vw,70px)/.91 var(--display);text-transform:uppercase;letter-spacing:-.05em}.catalog p{color:var(--muted);line-height:1.6}.category-grid{display:grid;grid-template-columns:repeat(3,1fr);border-top:2px solid var(--line);border-left:2px solid var(--line)}.category{padding:15px;border-right:2px solid var(--line);border-bottom:2px solid var(--line);background:var(--paper);font:800 11px/1.2 var(--body);text-transform:uppercase}.category b{display:block;font:950 26px/1 var(--display);margin-bottom:8px;color:var(--ink)}
  .final{padding:105px 0;background:var(--accent);border-bottom:2px solid var(--line)}.final .shell{display:grid;grid-template-columns:1fr auto;gap:50px;align-items:end}.final h2{max-width:840px;margin:0;font:950 clamp(48px,6vw,84px)/.87 var(--display);text-transform:uppercase;letter-spacing:-.055em}.final p{max-width:650px;line-height:1.55;margin:22px 0 0}.final .button{box-shadow:7px 7px 0 var(--paper)}
  footer{background:var(--ink);color:#eae7df;padding:34px 0}footer .shell{display:flex;justify-content:space-between;gap:24px;align-items:center;font:11px/1.4 var(--mono)}footer a{text-decoration:none;color:var(--accent)}
  @media(max-width:930px){.hero,.declaration .shell,.catalog-grid{grid-template-columns:1fr}.hero-copy{border-right:0;padding-right:0}.hero-panel{padding:35px 10px 65px 0}.steps{grid-template-columns:1fr 1fr}.step:nth-child(2){border-right:0}.step:nth-child(-n+2){border-bottom:2px solid var(--line)}.feature-grid{grid-template-columns:1fr 1fr}.metrics{grid-template-columns:1fr 1fr}.metric:nth-child(2){border-right:0}.metric:nth-child(-n+2){border-bottom:2px solid var(--line)}}
  @media(max-width:680px){.shell{width:min(100% - 26px,1200px)}.navlinks a:not(.button){display:none}.metrics,.steps,.feature-grid,.category-grid{grid-template-columns:1fr}.metric,.step{border-right:0;border-bottom:2px solid var(--line)}.metric:last-child,.step:last-child{border-bottom:0}.section-head,.final .shell{display:block}.final .button{margin-top:30px}.topology-row{grid-template-columns:1fr}.declaration .shell{gap:30px}.terminal-body{padding:18px;min-height:350px}.hero-copy{padding-top:58px}h1{font-size:55px}.category-grid{grid-template-columns:1fr 1fr}.catalog-grid{gap:30px}footer .shell{align-items:flex-start;flex-direction:column}}
"""


_SEARCH = ProductPage(
    slug="search",
    name="Distributed Search",
    eyebrow="Jina AI, wrapped for agents",
    title="The internet, already",
    title_accent="indexed for action.",
    lede="Search, read, rerank, embed, classify, segment, and verify through one agent-native surface—then federate the same question across your private knowledge systems.",
    primary_label="Connect the MCP →",
    primary_href="https://tools.distributed.systems/mcp",
    secondary_label="See the tool surface",
    secondary_href="#capabilities",
    accent="#86c8ff",
    accent_soft="#dbeeff",
    panel="#c5f45d",
    metrics=(
        ("100K", "Requests per minute", "Allocated Jina capacity ceiling"),
        ("18B", "Embedding tokens", "Reserved embedding capacity"),
        ("21", "Native agent tools", "Verified in this checkout"),
        ("4000", "Capacity horizon", "Long-lived allocation"),
    ),
    terminal_label="retrieval trace / dsco-search-041",
    terminal_html=r"""<span class="dim">$</span> <span class="prompt">distributed-search.search</span><br>
<span class="dim">query:</span> “autonomous publishing with accountable approval”
<div class="call"><b>01 · SEARCH</b><br>Fresh web results + structured citations<br><span class="dim">provider / jina · mode / live</span></div>
<div class="call"><b>02 · READ + SEGMENT</b><br>Clean source text, semantic chunks, stable URLs<br><span class="dim">6 documents · 42 evidence segments</span></div>
<div class="call"><b>03 · RERANK</b><br>Evidence ordered against the actual question<br><span class="dim">top_n / 8 · model / jina-reranker-v2</span></div>
<div class="result"><span class="ok">✓ retrieval packet ready</span><br>Sources preserved. Embeddings available. Private corpora can join the same query.</div>""",
    declaration="One retrieval spine. Every evidence surface.",
    declaration_detail="Distributed Search turns Jina's primitives into <b>composable agent operations</b>. The same call pattern can reach the open web, Research, Document QA, Video QA, Memory, and the registry.",
    steps=(
        ("01 / DISCOVER", "Search broadly", "Run web or federated search without making the agent learn a new provider contract."),
        ("02 / RESOLVE", "Read the source", "Convert URLs into clean, citation-preserving content instead of reasoning from snippets."),
        ("03 / ORDER", "Rerank evidence", "Put relevance ahead of chronology, SEO, or whichever result arrived first."),
        ("04 / REMEMBER", "Embed once", "Create durable vectors for downstream retrieval, classification, clustering, and memory."),
    ),
    features=(
        ("RETRIEVAL", "Search + read", "Fresh web retrieval and clean page reading through a consistent MCP contract."),
        ("RELEVANCE", "Rerank", "Reorder documents against the intent of the task before they reach the context window."),
        ("SEMANTICS", "Embed at scale", "Single and asynchronous batch embeddings backed by the allocated token reserve."),
        ("STRUCTURE", "Classify + segment", "Turn unstructured pages into labelled, bounded evidence units agents can actually use."),
        ("DEPTH", "DeepSearch", "Let Jina plan and execute deeper retrieval when one search pass is not enough."),
        ("FEDERATION", "Search all", "Fan one query into Research, Document QA, Video QA, Memory, and Registry siblings."),
    ),
    final_title="Give every agent a better first move.",
    final_copy="Start with evidence that is current, inspectable, and already shaped for the next operation.",
)


_RESEARCH = ProductPage(
    slug="research",
    name="Distributed Research",
    eyebrow="Parallel AI, made durable",
    title="Don't browse.",
    title_accent="Run the investigation.",
    lede="Launch parallel research, extraction, entity discovery, monitors, and synthesis as durable runs your agents can await, extend, group, and inspect.",
    primary_label="Connect the MCP →",
    primary_href="https://tools.distributed.systems/mcp",
    secondary_label="See the operating loop",
    secondary_href="#workflow",
    accent="#ff6a4d",
    accent_soft="#ffd9cf",
    panel="#f7c95e",
    metrics=(
        ("35", "Native agent tools", "Verified in this checkout"),
        ("N", "Parallel work lanes", "Search, extraction, entities"),
        ("∞", "Durable run horizon", "Await, extend, enrich, group"),
        ("24/7", "Monitor cadence", "Trigger and inspect events"),
    ),
    terminal_label="research run / run_9c72",
    terminal_html=r"""<span class="dim">$</span> <span class="prompt">distributed-research.deep_search</span><br>
<span class="dim">objective:</span> “Map the market, evidence, objections, and launch wedge.”
<div class="call"><b>LANE A · MARKET</b><br>Category map, companies, pricing, adoption signals<br><span class="ok">completed · 31 sources</span></div>
<div class="call"><b>LANE B · EVIDENCE</b><br>Primary-source extraction and contradiction checks<br><span class="ok">completed · 18 claims</span></div>
<div class="call"><b>LANE C · SENTIMENT</b><br>Objections, language, and audience response patterns<br><span class="hot">monitor attached · live</span></div>
<div class="result"><span class="ok">✓ synthesis ready</span><br>Run remains addressable. Inputs, sources, and lane results stay inspectable.</div>""",
    declaration="Research is a process, not a prompt.",
    declaration_detail="Distributed Research wraps Parallel into a <b>persistent operating surface</b>: tasks survive the turn, monitors keep watching, and synthesis can cite the runs that produced it.",
    steps=(
        ("01 / SPECIFY", "Frame the work", "Turn an objective into a task spec, processor, or reusable research template."),
        ("02 / FAN OUT", "Run in parallel", "Search, extract, FindAll, and entity work proceed as independent durable lanes."),
        ("03 / DEVELOP", "Extend the run", "Await, enrich, group, or continue a run without losing its inputs and evidence."),
        ("04 / SYNTHESIZE", "Make the answer", "Compose across completed runs while preserving the trace back to sources."),
    ),
    features=(
        ("INVESTIGATION", "Deep search", "Create an asynchronous research run and inspect it while work progresses."),
        ("DISCOVERY", "FindAll", "Enumerate companies, people, products, or other entities—then enrich and extend the set."),
        ("EXTRACTION", "Structured data", "Pull schema-shaped data from sources instead of manually normalizing prose."),
        ("CONTINUITY", "Monitors", "Create recurring research monitors and inspect their event history on demand."),
        ("OPERATIONS", "Runs + groups", "Await, list, cancel, group, and retrieve the exact input behind any research job."),
        ("JUDGMENT", "Synthesis", "Combine selected runs into a deliberate final brief rather than a context-window collage."),
    ),
    final_title="Turn open-ended questions into managed work.",
    final_copy="Your agents get parallelism without losing run state, source lineage, or the ability to continue later.",
)


_TOOLS = ProductPage(
    slug="tools",
    name="Distributed Tools",
    eyebrow="The integration plane for agents",
    title="One call.",
    title_accent="The whole company.",
    lede="Discover and execute the right operation across first-party MCP servers, configured providers, and the wider preset catalog—through one governed control plane.",
    primary_label="Open the MCP endpoint →",
    primary_href="https://tools.distributed.systems/mcp",
    secondary_label="Inspect the architecture",
    secondary_href="#architecture",
    accent="#c5f45d",
    accent_soft="#e4f7b9",
    panel="#9bc9ff",
    metrics=(),
    terminal_label="execution trace / exec_0f31",
    terminal_html=r"""<span class="dim">$</span> <span class="prompt">distributed-tools.discover_tools</span><br>
<span class="dim">intent:</span> “Research the topic, draft the post, and prepare it for approval.”
<div class="call"><b>RETRIEVE</b><br>Semantic tool match across the active catalog<br><span class="dim">policy / tenant · mode / governed</span></div>
<div class="call"><b>PLAN</b><br>distributed-search → distributed-research → distributed-publish<br><span class="dim">3 servers · explicit data boundaries</span></div>
<div class="call"><b>EXECUTE</b><br>Credential resolution, confirmation gates, retries, receipts<br><span class="ok">ready · human approval preserved</span></div>
<div class="result"><span class="ok">✓ workflow assembled</span><br>The harness sees a coherent capability surface—not 812 separate integration manuals.</div>""",
    declaration="The abstraction above every integration.",
    declaration_detail="Distributed Tools is where registration becomes execution: <b>discovery, credentials, policy, routing, and receipts</b> stay coherent while the underlying servers remain independently deployable.",
    steps=(
        ("01 / EXPRESS", "Describe intent", "DSCO CLI, Penumbra, or another client asks for the operation it needs."),
        ("02 / DISCOVER", "Retrieve tools", "The registry narrows the active catalog by semantics, tenancy, and capability."),
        ("03 / GOVERN", "Apply the boundary", "Credentials, confirmation requirements, and execution policy are resolved before dispatch."),
        ("04 / EXECUTE", "Call the server", "The selected integration runs with retries, tracing, feedback, and a stable result contract."),
    ),
    features=(
        ("REGISTRY", "Announce + heartbeat", "Integration servers register their tools and keep health state fresh."),
        ("DISCOVERY", "Retrieve by intent", "Agents search a large capability space without injecting the entire schema into context."),
        ("EXECUTION", "One gateway", "Route calls to native and remote MCP backends through a stable API and MCP surface."),
        ("IDENTITY", "Tenant credentials", "Resolve connected accounts without disclosing secrets to the agent or model."),
        ("GOVERNANCE", "Confirmation gates", "High-impact operations can require explicit authorization before they leave the control plane."),
        ("FEEDBACK", "Learn from calls", "Capture execution outcomes so retrieval and routing improve from real use."),
    ),
    final_title="Stop wiring agents to APIs one at a time.",
    final_copy="Connect the harness once. Register every server behind a control plane designed for discovery, governance, and execution.",
)


_NATIVE_SERVERS = (
    "Search", "Research", "Publish", "Document QA", "Video QA", "Memory",
    "DSCO Bridge", "Swarm", "Finance Fabric", "Integration Fabric",
    "Registry Wrapper", "Timeline", "Unit Fabric", "Codex Apps",
)

_CATEGORIES = (
    ("65", "Databases"), ("25", "Finance"), ("24", "Security"),
    ("22", "Analytics"), ("22", "Healthcare"), ("21", "Cloud"),
    ("18", "Data engineering"), ("16", "AI tools"), ("16", "Geospatial"),
    ("16", "Infrastructure"), ("16", "Media"), ("16", "Search"),
)


def _nav(current: str) -> str:
    links = (
        ("Search", "/search"), ("Research", "/research"),
        ("Publish", "/publish"), ("Tools", "/tools"),
        ("Router ↗", "https://router.distributed.systems"),
    )
    return "".join(
        f'<a href="{href}"' + (' aria-current="page"' if href == f"/{current}" else "") + f'>{label}</a>'
        for label, href in links
    )


def _metrics(items: tuple[tuple[str, str, str], ...]) -> str:
    return "".join(
        f'<article class="metric"><div class="value">{escape(value)}</div>'
        f'<div class="label">{escape(label)}</div><div class="note">{escape(note)}</div></article>'
        for value, label, note in items
    )


def _topology() -> str:
    rows = (
        ("Agent harnesses", ("dsco-cli", "dsco-penumbra", "OpenAI MCP clients", "Custom agents")),
        ("Higher-order routing", ("DSCO Router", "Chimera policy", "Provider fallbacks", "Parallel synthesis")),
        ("Tool control plane", ("Discovery", "Execution", "Credentials", "Confirmations", "Receipts", "Feedback")),
        ("First-party servers", _NATIVE_SERVERS),
        ("Integration catalog", ("812 presets", "173 provider definitions", "15,344 declared operations", "149 enterprise-critical")),
    )
    rendered = []
    for label, chips in rows:
        rendered.append(
            f'<div class="topology-row"><div class="topology-label">{escape(label)}</div><div class="chips">'
            + "".join(f'<span class="chip">{escape(chip)}</span>' for chip in chips)
            + "</div></div>"
        )
    return '<div class="topology" id="architecture">' + "".join(rendered) + "</div>"


def _catalog() -> str:
    categories = "".join(
        f'<div class="category"><b>{count}</b>{escape(label)}</div>'
        for count, label in _CATEGORIES
    )
    return f"""
  <section class="catalog" id="catalog">
    <div class="shell catalog-grid">
      <div><span class="eyebrow">The checked-in catalog</span><h2>Broad by design. Narrow at call time.</h2><p>These are source-verified definitions in this ToolManagement checkout—not a decorative logo wall. The live registry can activate a smaller tenant-specific set while preserving the same discovery contract.</p><a class="button light" href="/services">Inspect live services →</a></div>
      <div class="category-grid">{categories}</div>
    </div>
  </section>"""


def _render(page: ProductPage, nonce: str, *, metrics: tuple[tuple[str, str, str], ...] | None = None) -> str:
    page_metrics = page.metrics if metrics is None else metrics
    steps = "".join(
        f'<article class="step"><span class="step-num">{escape(num)}</span><h3>{escape(title)}</h3><p>{escape(copy)}</p></article>'
        for num, title, copy in page.steps
    )
    features = "".join(
        f'<article class="feature"><span class="tag">{escape(tag)}</span><h3>{escape(title)}</h3><p>{escape(copy)}</p></article>'
        for tag, title, copy in page.features
    )
    tools_only = (_topology() + _catalog()) if page.slug == "tools" else ""
    html = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="description" content="__DESCRIPTION__"><meta name="theme-color" content="__ACCENT__">
<title>__NAME__ — Distributed Systems</title>
<style nonce="__NONCE__">__STYLE__</style>
</head><body>
<header class="shell nav">
  <a class="brand" href="/"><span class="mark">D/S</span><span>__NAME__</span></a>
  <nav class="navlinks" aria-label="Product family">__NAV__<a class="button" href="/account">Account</a></nav>
</header>
<main>
  <section class="shell hero">
    <div class="hero-copy"><span class="eyebrow">__EYEBROW__</span><h1>__TITLE__ <em>__TITLE_ACCENT__</em></h1><p class="lede">__LEDE__</p>
      <div class="actions"><a class="button" href="__PRIMARY_HREF__">__PRIMARY_LABEL__</a><a class="button light" href="__SECONDARY_HREF__">__SECONDARY_LABEL__</a><span class="proof">Production surface · MCP-native · designed for agent harnesses</span></div>
    </div>
    <div class="hero-panel"><div class="terminal"><div class="terminal-top"><span>__TERMINAL_LABEL__</span><span class="lights"><i></i><i></i><i></i></span></div><div class="terminal-body">__TERMINAL__</div></div></div>
  </section>
  <section class="shell metrics">__METRICS__</section>
  <section class="declaration"><div class="shell"><div><span class="eyebrow">The product thesis</span><h2>__DECLARATION__</h2></div><p>__DECLARATION_DETAIL__</p></div></section>
  <section class="section shell" id="workflow"><div class="section-head"><div><span class="eyebrow">The operating loop</span><h2>From intent<br>to inspected result.</h2></div><p>Each layer has a clear job. Agents move quickly without hiding where evidence, credentials, policy, or judgment entered the run.</p></div><div class="steps">__STEPS__</div></section>
  <section class="section shell" id="capabilities"><div class="section-head"><div><span class="eyebrow">Native capability surface</span><h2>Built for composition,<br>not demos.</h2></div><p>The server exposes small, explicit operations that can be chained by DSCO, Penumbra, or any MCP-capable agent.</p></div><div class="feature-grid">__FEATURES__</div>__TOPOLOGY__</section>
  __CATALOG__
  <section class="final"><div class="shell"><div><h2>__FINAL_TITLE__</h2><p>__FINAL_COPY__</p></div><a class="button" href="__PRIMARY_HREF__">Connect now →</a></div></section>
</main>
<footer><div class="shell"><span>Distributed Systems · infrastructure for agent work</span><span><a href="/docs">API docs</a> · <a href="https://router.distributed.systems">Router</a> · <a href="/account">Account</a></span></div></footer>
</body></html>"""
    replacements = {
        "__DESCRIPTION__": escape(page.lede, quote=True), "__ACCENT__": page.accent,
        "__NAME__": escape(page.name), "__NONCE__": escape(nonce, quote=True),
        "__STYLE__": _STYLE.replace("__ACCENT__", page.accent).replace("__ACCENT_SOFT__", page.accent_soft).replace("__PANEL__", page.panel),
        "__NAV__": _nav(page.slug), "__EYEBROW__": escape(page.eyebrow),
        "__TITLE__": escape(page.title), "__TITLE_ACCENT__": escape(page.title_accent),
        "__LEDE__": escape(page.lede), "__PRIMARY_HREF__": escape(page.primary_href, quote=True),
        "__PRIMARY_LABEL__": escape(page.primary_label), "__SECONDARY_HREF__": escape(page.secondary_href, quote=True),
        "__SECONDARY_LABEL__": escape(page.secondary_label), "__TERMINAL_LABEL__": escape(page.terminal_label),
        "__TERMINAL__": page.terminal_html, "__METRICS__": _metrics(page_metrics),
        "__DECLARATION__": escape(page.declaration), "__DECLARATION_DETAIL__": page.declaration_detail,
        "__STEPS__": steps, "__FEATURES__": features, "__TOPOLOGY__": _topology() if page.slug == "tools" else "",
        "__CATALOG__": _catalog() if page.slug == "tools" else "", "__FINAL_TITLE__": escape(page.final_title),
        "__FINAL_COPY__": escape(page.final_copy),
    }
    for marker, value in replacements.items():
        html = html.replace(marker, value)
    return html


def render_search_marketing(nonce: str) -> str:
    return _render(_SEARCH, nonce)


def render_research_marketing(nonce: str) -> str:
    return _render(_RESEARCH, nonce)


def render_tools_marketing(
    nonce: str,
    *,
    active_tools: int,
    active_backends: int,
    healthy_backends: int,
) -> str:
    metrics = (
        (f"{int(active_tools):,}", "Active tool operations", "Live registry union at render time"),
        ("812", "Preset integrations", "Verified checked-in catalog"),
        ("15,344", "Declared operations", "Across the preset catalog"),
        ("14", "First-party servers", f"{int(healthy_backends)}/{int(active_backends)} registered backends healthy"),
    )
    return _render(_TOOLS, nonce, metrics=metrics)


__all__ = [
    "PRODUCT_PATHS", "publish_page_headers", "render_search_marketing",
    "render_research_marketing", "render_tools_marketing",
]
