from __future__ import annotations

import json
from pathlib import Path
import sys

from playwright.sync_api import sync_playwright


BASE_URL = "http://127.0.0.1:8876/"
ARTIFACT = Path("/tmp/gen0-universe-smoke.png")


def main() -> None:
    page_errors: list[str] = []
    console_errors: list[str] = []
    with sync_playwright() as playwright:
        launch: dict[str, object] = {"headless": True}
        # Keep the smoke test usable when the Python Playwright package has
        # advanced beyond an already-cached Chromium revision on a dev host.
        cached = sorted((Path.home() / "Library/Caches/ms-playwright").glob("chromium-*/chrome-mac/Chromium.app/Contents/MacOS/Chromium"), reverse=True)
        if cached:
            launch["executable_path"] = str(cached[0])
        browser = playwright.chromium.launch(**launch)
        page = browser.new_page(viewport={"width": 1720, "height": 1080}, device_scale_factor=1)
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        page.on("console", lambda message: console_errors.append(message.text) if message.type == "error" and "favicon" not in message.text.lower() else None)

        page.goto(BASE_URL, wait_until="networkidle")
        page.locator("#world").wait_for(state="visible")
        page.wait_for_function("window.factoryUniverse && window.factoryUniverse.productionSnapshot")
        assert page.title() == "A.COLLE // REPLICATION FIELD GEN-0"
        assert page.locator(".universe-view").count() == 7
        assert page.locator("#fleet-value").inner_text() in {"72", "100"}

        # The headless rack must connect and render a recorded run rather than a
        # local placeholder.
        page.locator('[data-universe="SIMLAB"]').click()
        page.locator("#sim-lab.open").wait_for()
        page.wait_for_function("document.querySelector('#sim-service-state').textContent !== 'CONNECTING'")
        assert page.locator("#sim-service-state").inner_text() in {"SERVICE READY", "RUN RECORDED"}
        page.locator(".sim-kpi").first.wait_for(timeout=20_000)
        assert "ALL INVARIANTS PASS" in page.locator("#sim-results").inner_text()
        assert "MUJOCO" in page.locator("#sim-results").inner_text().upper()
        c2 = page.locator("#sim-results .gate-row").filter(has_text="GC2")
        c3 = page.locator("#sim-results .gate-row").filter(has_text="GC3")
        c4 = page.locator("#sim-results .gate-row").filter(has_text="GC4")
        assert "PASS" in c2.inner_text()
        assert "PASS" in c3.inner_text()
        assert "BLOCKED" in c4.inner_text()
        assert page.locator("#gpu-rtl-step b").inner_text() == "PASS"

        # Exercise the browser-to-API POST path as well as the preloaded latest
        # artifact. A 120 minute run is enough for UI plumbing and finishes fast.
        page.locator("#sim-minutes").fill("120")
        page.locator("#sim-run").click()
        page.wait_for_function("!document.querySelector('#sim-run').disabled", timeout=30_000)
        assert page.locator("#sim-service-state").inner_text() == "RUN RECORDED"
        assert "RUN REJECTED" not in page.locator("#sim-results").inner_text()
        page.locator("#sim-close").click()

        # Ground robotics is a build-and-use program, not a disconnected art
        # layer: the dossier must expose products, routes, permits and live units.
        page.locator('[data-universe="ROBOTICS"]').click()
        page.locator("#dossier.open").wait_for()
        robotics_text = page.locator("#dossier-content").inner_text()
        assert "HMX-1" in robotics_text
        assert "HEX-6" in robotics_text
        assert "S1" in robotics_text
        assert "CL-4" in robotics_text
        assert "25 KG HMX TOTE" in robotics_text.upper()
        page.locator("#dossier-close").click()

        # Every visible field aircraft has a Q9 record and a physically bounded
        # gross-mass readout. Find a released carrier and issue deployment.
        page.locator("#cell-inspector").wait_for()
        selected = None
        for _ in range(100):
            if page.locator("#cell-payload-action").is_enabled():
                selected = page.locator("#cell-id").inner_text()
                break
            page.locator("#cell-next").click()
        assert selected is not None, "no field aircraft passed the visible Q9 release gate"
        assert float(page.locator("#cell-mass").inner_text().split()[0]) <= 3.6
        page.locator("#cell-payload-action").click()
        page.wait_for_function("!document.querySelector('#cell-q9').textContent.includes('STOWED')", timeout=5_000)

        # Preserved detailed multi-camera airframe experience.
        page.locator('[data-universe="AIRFRAME"]').click()
        airframe = page.frame_locator("#portal-airframe")
        airframe.locator("#perspective").wait_for(timeout=10_000)
        assert "2.828 KG" in airframe.locator("#auw-caption").inner_text()
        assert "0.772 KG" in airframe.locator("#margin-caption").inner_text()
        assert airframe.locator("#exploded").is_visible()
        airframe.locator("#payload-action").click()
        page.wait_for_timeout(500)
        assert airframe.locator("#payload-state").inner_text() in {"DEPLOYING", "GROUND"}

        # Preserved Arthur / Itano experience remains an interactive iframe.
        page.locator('[data-universe="ITANO"]').click()
        classic = page.frame_locator("#portal-itano")
        classic.locator("#screen").wait_for(timeout=10_000)
        assert classic.locator("#title").inner_text() == "ITANO CIRCUS"
        classic.locator("#screen").click(position={"x": 200, "y": 120})
        page.wait_for_timeout(250)
        assert int(classic.locator("#s-msl").inner_text()) > 0

        page.locator('[data-universe="FIELD"]').click()
        page.locator("#world").wait_for()
        completed = page.evaluate("window.factoryUniverse.runFullShift(100, 2400)")
        assert completed["mission"]["status"] == "COMPLETE", completed["mission"]
        assert completed["mission"]["physicalInvariants"]["allHaveQ9"]
        assert completed["mission"]["physicalInvariants"]["noMtowViolation"]
        assert completed["production"]["status"] == "COMPLETE", completed["production"]
        assert completed["production"]["finished"] == 7
        assert completed["production"]["robotics"]["count"] == 15
        assert completed["production"]["robotics"]["blockProof"]
        assert len(completed["production"]["robotics"]["commissioned"]) == 4
        assert all(robot["postBuildWorkComplete"] for robot in completed["production"]["robotics"]["commissioned"])
        assert all(completed["production"]["invariants"].values()), completed["production"]["invariants"]
        page.screenshot(path=str(ARTIFACT), full_page=True)
        browser.close()

    if page_errors or console_errors:
        raise AssertionError(json.dumps({"page_errors": page_errors, "console_errors": console_errors}, indent=2))
    print(json.dumps({
        "status": "PASS", "url": BASE_URL, "selected_q9_carrier": selected,
        "screenshot": str(ARTIFACT), "page_errors": 0, "console_errors": 0,
    }, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"BROWSER SMOKE FAILED: {error}", file=sys.stderr)
        raise
