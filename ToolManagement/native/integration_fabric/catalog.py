"""
Integration Fabric catalog.

Declares a broad surface of SaaS / API operations as (category, operation, kind)
specs plus the set of providers per category. The cartesian product
provider x operation yields the concrete MCP tools the fabric exposes.

Unlike the finance fabric (which has bespoke per-domain handlers), every
operation here is classified by a small set of CRUD-ish *kinds*. A single
generic handler in the server shapes a deterministic, kind-appropriate response
for any (category, op, provider), so the entire surface is exercisable in
sandbox mode and any provider that has a live adapter can override.

Kinds:
  create  -> make a new resource (returns id + stored object)
  read    -> fetch a resource by id
  list    -> page a collection
  update  -> mutate a resource by id
  delete  -> remove / void / cancel a resource by id
  action  -> a verb on a resource or service (send, merge, trigger, resolve...)
  search  -> a query returning ranked results
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


# (operation, kind)
OpSpec = Tuple[str, str]


CATEGORY_OPS: Dict[str, List[OpSpec]] = {
    "communication": [
        ("send_message", "action"), ("list_messages", "list"), ("get_message", "read"),
        ("create_channel", "create"), ("list_channels", "list"), ("upload_file", "action"),
        ("add_reaction", "action"), ("search", "search"),
    ],
    "email": [
        ("send", "action"), ("get", "read"), ("list", "list"), ("template_create", "create"),
        ("template_list", "list"), ("stats", "read"), ("validate", "action"), ("suppress", "action"),
    ],
    "sms_voice": [
        ("send_sms", "action"), ("send_mms", "action"), ("make_call", "action"),
        ("get_message", "read"), ("list_messages", "list"), ("lookup_number", "read"),
        ("buy_number", "create"), ("list_numbers", "list"),
    ],
    "calendar": [
        ("create_event", "create"), ("get_event", "read"), ("list_events", "list"),
        ("update_event", "update"), ("delete_event", "delete"), ("freebusy", "read"),
        ("list_calendars", "list"),
    ],
    "crm": [
        ("create_contact", "create"), ("get_contact", "read"), ("list_contacts", "list"),
        ("update_contact", "update"), ("create_deal", "create"), ("list_deals", "list"),
        ("create_company", "create"), ("search", "search"),
    ],
    "support": [
        ("create_ticket", "create"), ("get_ticket", "read"), ("list_tickets", "list"),
        ("update_ticket", "update"), ("add_comment", "action"), ("close_ticket", "delete"),
        ("search", "search"),
    ],
    "project": [
        ("create_issue", "create"), ("get_issue", "read"), ("list_issues", "list"),
        ("update_issue", "update"), ("transition_issue", "action"), ("create_project", "create"),
        ("add_comment", "action"), ("search", "search"),
    ],
    "docs": [
        ("create_page", "create"), ("get_page", "read"), ("update_page", "update"),
        ("list_pages", "list"), ("append_block", "action"), ("search", "search"),
    ],
    "storage": [
        ("upload", "action"), ("download", "read"), ("list", "list"), ("delete", "delete"),
        ("get_metadata", "read"), ("create_folder", "create"), ("share_link", "action"),
        ("move", "action"),
    ],
    "vcs": [
        ("create_repo", "create"), ("get_repo", "read"), ("list_repos", "list"),
        ("create_issue", "create"), ("create_pr", "create"), ("list_prs", "list"),
        ("merge_pr", "action"), ("list_commits", "list"), ("create_branch", "create"),
        ("get_file", "read"),
    ],
    "ci": [
        ("trigger_build", "action"), ("get_build", "read"), ("list_builds", "list"),
        ("cancel_build", "delete"), ("list_pipelines", "list"), ("get_artifacts", "read"),
    ],
    "cloud": [
        ("create_instance", "create"), ("get_instance", "read"), ("list_instances", "list"),
        ("terminate_instance", "delete"), ("start_instance", "action"), ("stop_instance", "action"),
        ("list_regions", "list"),
    ],
    "monitoring": [
        ("submit_metric", "action"), ("query_metric", "search"), ("create_monitor", "create"),
        ("list_monitors", "list"), ("get_dashboard", "read"), ("list_alerts", "list"),
    ],
    "incident": [
        ("create_incident", "create"), ("get_incident", "read"), ("list_incidents", "list"),
        ("ack_incident", "action"), ("resolve_incident", "action"), ("list_oncall", "list"),
    ],
    "ecommerce": [
        ("create_product", "create"), ("get_product", "read"), ("list_products", "list"),
        ("update_product", "update"), ("create_order", "create"), ("get_order", "read"),
        ("list_orders", "list"), ("fulfill_order", "action"), ("list_customers", "list"),
    ],
    "maps": [
        ("geocode", "read"), ("reverse_geocode", "read"), ("directions", "read"),
        ("distance_matrix", "read"), ("places_search", "search"), ("place_details", "read"),
        ("timezone", "read"),
    ],
    "social": [
        ("post", "create"), ("get_post", "read"), ("list_posts", "list"), ("delete_post", "delete"),
        ("get_profile", "read"), ("list_followers", "list"), ("upload_media", "action"),
        ("search", "search"),
    ],
    "analytics": [
        ("track_event", "action"), ("get_report", "read"), ("list_metrics", "list"),
        ("create_funnel", "create"), ("get_user", "read"), ("identify", "action"),
    ],
    "hr": [
        ("list_employees", "list"), ("get_employee", "read"), ("create_employee", "create"),
        ("list_timeoff", "list"), ("run_payroll", "action"), ("get_payslip", "read"),
    ],
    "esign": [
        ("create_envelope", "create"), ("send_envelope", "action"), ("get_envelope", "read"),
        ("list_envelopes", "list"), ("void_envelope", "delete"), ("download_document", "read"),
    ],
    "video": [
        ("create_meeting", "create"), ("get_meeting", "read"), ("list_meetings", "list"),
        ("update_meeting", "update"), ("delete_meeting", "delete"), ("list_recordings", "list"),
    ],
    "ai": [
        ("complete", "action"), ("chat", "action"), ("embed", "action"), ("image_generate", "action"),
        ("transcribe", "action"), ("moderate", "action"), ("classify", "action"),
    ],
    "search_engine": [
        ("query", "search"), ("index_document", "create"), ("delete_document", "delete"),
        ("suggest", "search"), ("autocomplete", "search"),
    ],
    "weather": [
        ("current", "read"), ("forecast", "read"), ("history", "read"), ("alerts", "list"),
        ("air_quality", "read"),
    ],
    "forms": [
        ("create_form", "create"), ("get_form", "read"), ("list_forms", "list"),
        ("list_responses", "list"), ("get_response", "read"),
    ],
    "translation": [
        ("translate", "action"), ("detect_language", "action"), ("list_languages", "list"),
        ("glossary_create", "create"),
    ],
    "knowledge": [
        ("create_article", "create"), ("get_article", "read"), ("list_articles", "list"),
        ("update_article", "update"), ("search", "search"),
    ],
    "media": [
        ("system_status", "read"), ("search", "search"), ("list_items", "list"),
        ("add_item", "create"), ("list_queue", "list"), ("list_history", "list"),
        ("trigger_search", "action"),
    ],
    "econ": [
        ("series", "read"), ("observations", "list"), ("search", "search"),
        ("releases", "list"), ("categories", "list"),
    ],
}


CATEGORY_PROVIDERS: Dict[str, List[str]] = {
    "communication": ["slack", "teams", "discord", "mattermost", "rocketchat", "zulip"],
    "email": ["sendgrid", "mailgun", "ses", "postmark", "sparkpost", "resend"],
    "sms_voice": ["twilio", "vonage", "messagebird", "plivo", "bandwidth", "sinch"],
    "calendar": ["google_calendar", "outlook_calendar", "calendly", "cronofy", "nylas"],
    "crm": ["salesforce", "hubspot", "pipedrive", "zoho_crm", "close", "copper"],
    "support": ["zendesk", "intercom", "freshdesk", "help_scout", "front", "gorgias"],
    "project": ["jira", "asana", "linear", "trello", "monday", "clickup", "shortcut"],
    "docs": ["notion", "confluence", "google_docs", "coda", "slite", "nuclino"],
    "storage": ["s3", "gcs", "azure_blob", "dropbox", "box", "google_drive", "onedrive"],
    "vcs": ["github", "gitlab", "bitbucket", "azure_repos", "gitea"],
    "ci": ["github_actions", "circleci", "jenkins", "gitlab_ci", "travis", "buildkite"],
    "cloud": ["aws", "gcp", "azure", "digitalocean", "linode", "vultr", "hetzner"],
    "monitoring": ["datadog", "newrelic", "grafana", "prometheus", "honeycomb", "dynatrace"],
    "incident": ["pagerduty", "opsgenie", "victorops", "incident_io", "firehydrant"],
    "ecommerce": ["shopify", "woocommerce", "bigcommerce", "magento", "squarespace", "wix"],
    "maps": ["google_maps", "mapbox", "here", "tomtom", "openrouteservice"],
    "social": ["twitter", "facebook", "instagram", "linkedin", "reddit", "youtube", "tiktok"],
    "analytics": ["ga4", "mixpanel", "amplitude", "segment", "posthog", "heap"],
    "hr": ["workday", "bamboohr", "gusto", "rippling", "deel", "namely"],
    "esign": ["docusign", "dropbox_sign", "pandadoc", "adobe_sign", "signnow"],
    "video": ["zoom", "google_meet", "webex", "teams", "whereby"],
    "ai": ["openai", "anthropic", "cohere", "huggingface", "replicate", "mistral",
           "together", "groq", "xai", "deepseek", "openrouter", "jina"],
    "search_engine": ["algolia", "elastic", "typesense", "meilisearch", "opensearch", "bing"],
    "weather": ["openweather", "weatherapi", "tomorrow", "visualcrossing", "accuweather"],
    "forms": ["typeform", "google_forms", "jotform", "surveymonkey", "tally"],
    "translation": ["deepl", "google_translate", "aws_translate", "azure_translator", "modernmt"],
    "knowledge": ["notion", "confluence", "guru", "zendesk_guide", "document360"],
    "media": ["sonarr", "radarr", "lidarr", "prowlarr", "bazarr", "overseerr",
              "sabnzbd", "nzbhydra2"],
    "econ": ["fred", "alphavantage", "worldbank", "imf", "oecd"],
}


# Input schema fragments by kind.
def _schema_for(kind: str) -> Tuple[Dict[str, Any], List[str]]:
    if kind == "create":
        return ({"data": {"type": "object", "description": "resource fields"},
                 "idempotency_key": {"type": "string"}}, [])
    if kind == "read":
        return ({"id": {"type": "string", "description": "resource id or natural key"}}, ["id"])
    if kind == "list":
        return ({"limit": {"type": "integer", "default": 20},
                 "cursor": {"type": "string"},
                 "query": {"type": "string", "description": "optional filter"}}, [])
    if kind == "update":
        return ({"id": {"type": "string"}, "data": {"type": "object"}}, ["id"])
    if kind == "delete":
        return ({"id": {"type": "string"}}, ["id"])
    if kind == "action":
        return ({"id": {"type": "string", "description": "target id (optional)"},
                 "params": {"type": "object", "description": "action parameters"}}, [])
    if kind == "search":
        return ({"query": {"type": "string"}, "limit": {"type": "integer", "default": 10}}, ["query"])
    return ({}, [])


def _describe(category: str, op: str, kind: str) -> str:
    verb = op.replace("_", " ")
    return f"{verb} ({kind}) in {category}"


def build_tools() -> Tuple[List[Dict[str, Any]], Dict[str, Tuple[str, str, str, str]]]:
    """L0 provider primitives: ``{provider}_{category}_{op}``.

    dispatch_map: tool_name -> (category, op, kind, provider).
    """
    tools: List[Dict[str, Any]] = []
    dispatch: Dict[str, Tuple[str, str, str, str]] = {}
    for category, ops in CATEGORY_OPS.items():
        providers = CATEGORY_PROVIDERS.get(category, [])
        for provider in providers:
            for op, kind in ops:
                name = f"{provider}_{category}_{op}"
                props, required = _schema_for(kind)
                tools.append({
                    "name": name,
                    "description": f"[{provider}] {_describe(category, op, kind)}",
                    "inputSchema": {"type": "object", "properties": props,
                                    **({"required": required} if required else {})},
                    # Carry taxonomy so the registry can filter/rank by category
                    # and provider instead of defaulting everything to 'general'.
                    "category": category,
                    "tags": [category, provider, op, kind, "integration"],
                })
                dispatch[name] = (category, op, kind, provider)
    return tools, dispatch


def default_provider(category: str) -> str:
    return CATEGORY_PROVIDERS[category][0]


def build_canonical() -> Tuple[List[Dict[str, Any]], Dict[str, Tuple[str, str, str, str]]]:
    """L1 canonical operations: ``{category}__{op}`` -> default provider."""
    tools: List[Dict[str, Any]] = []
    dispatch: Dict[str, Tuple[str, str, str, str]] = {}
    for category, ops in CATEGORY_OPS.items():
        provider = default_provider(category)
        for op, kind in ops:
            name = f"{category}__{op}"
            props, required = _schema_for(kind)
            tools.append({
                "name": name,
                "description": f"[canonical:{category}] {_describe(category, op, kind)} "
                               f"(default provider: {provider})",
                "inputSchema": {"type": "object", "properties": props,
                                **({"required": required} if required else {})},
                "category": category,
                "tags": [category, op, kind, "canonical", "integration"],
            })
            dispatch[name] = (category, op, kind, provider)
    return tools, dispatch
