# GEN-0 simulation service

This service is the authoritative headless model behind the browser universe. It
uses SimPy for factory, aircraft, Q9, and mobile-ground-robot discrete-event
simulation and MuJoCo for a small, explicit quadruped contact-validation case.
It does **not** claim that a discrete-event or rigid-body model is certified
flight or industrial-robot dynamics.

Run it from `itano-circus`:

```sh
uv sync --project simulation
simulation/.venv/bin/python -m simulation.server
```

Then open <http://127.0.0.1:8876/>. The service serves the three preserved HTML
experiences and exposes the simulation API beneath `/api`.

The port-8700 DSCO Superapp mounts those same files at `/factory-universe/` and
proxies its simulation calls through `/api/proxy/factory_universe/api`. On this
workstation, `simulation/launchd/com.dsco.factory-simulation.plist` is installed
as the `com.dsco.factory-simulation` user LaunchAgent so the rack starts with the
Superapp and is restarted if it exits.

Useful checks:

```sh
simulation/.venv/bin/python -m unittest discover -s simulation/tests -v
simulation/.venv/bin/python -m simulation.cli --fleet 100 --minutes 720 --seed 903742
simulation/.venv/bin/python silicon/tools/run_rtl.py
simulation/.venv/bin/python silicon/tools/run_synth.py
```

Every API run is written to `runs/<run-id>.json`. Each artifact includes the
scenario, published/assumed parameters, role roster, event trace, physical and
governance invariants, factory-operation records, and the current local silicon
tool capability probe.

Version 0.3 adds four mobile-robot product and operating models:

- HMX-1 full-size humanoid: 69.5 kg, 27 DOF, 15.9 kg released payload.
- HEX-6 six-legged industrial inspector: 26 kg, 18 DOF, 10 kg payload.
- S1 ten-module confined-space inspector: 30.5 kg, tethered, LOTO-gated.
- CL-4 wall/vessel crawler: 16.5 kg, tether- and exclusion-zone-gated.

The 720-minute reference run begins with 11 operating ground robots, routes
four first articles through 25 additional manufacturing operations, and closes
only after each commissioned unit performs a safety-audited factory task. One
deliberately overweight HMX request is submitted to prove that management cannot
override the independent payload gate. The output `robotics` object contains
the roster, task records, permit audits, blocks, energy, and post-build proof.

The robot figures are Gen-0 design targets. Published commercial and research
systems bound selected dimensions or payload classes; they are not represented
as locally reproduced products. Actuators, brakes, bearings, sensors, safety
controllers, battery cells, compute silicon, tethers, and NDT probes remain
explicit purchased dependencies.
