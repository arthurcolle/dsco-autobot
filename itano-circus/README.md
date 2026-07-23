# A.COLLE GEN-0 factory universe

`index.html` is the integrated factory universe. It keeps the detailed M100/Q9
airframe experience in `aircell.html` and the original pixel Arthur / Itano
Circus in `classic.html`, while adding an 18-facility cyber-physical factory,
a 40–100 aircraft swarm, a mass-conserving production ledger, a bounded GPU
program, and working mobile robotics.

## Mobile robotics program

The current shift contains seven serialized products and 44 routed operations.
Four are ground robots:

| Product | Physical target | Released work | Final acceptance |
| --- | --- | --- | --- |
| HMX-1 | 1.78 m, 69.5 kg, 27 DOF, 2.4 kWh | Tote transfer, machine tending, kit presentation, AMR handoff, rework assist | Brake hold, insulation, mass/CG, 15.9 kg lift, balance recovery, CAT1 stop, dock |
| HEX-6 | 26 kg, 18 DOF, 0.95 kWh | Rough-floor, thermal, leak, and stair inspection | 10 kg proof load, seal, steps, slope hold, sensor correlation |
| S1 | 3.0 m, ten modules, 30.5 kg, tethered | Under-machine, cable-trench, and duct inspection | Seal, bend, tether proof, LOTO token, deadman stop, recovery |
| CL-4 | 16.5 kg, four adhesion pods, tethered | Busway thermography, duct wall scan, vessel ultrasound | Adhesion margin, arrest tether, NDT calibration, vertical traverse, power-loss catch |

The browser starts with 11 operating ground robots. One of each family is built
from released inventory during the shift. Serialization is not enough to close
the shift: each new unit must pass a task safety audit and perform its first
factory job. An intentionally invalid 25 kg HMX request is blocked against the
15.9 kg release limit and remains visible in the dossier and simulation result.

The BOM distinguishes local frames, covers, harnesses, fixtures, integration,
and software from purchased actuators, brakes, precision bearings, sensors,
safety PLCs, cells, compute silicon, tether hardware, and NDT instruments.

## Run and verify

From this directory:

```sh
uv sync --project simulation
simulation/.venv/bin/python -m simulation.server
```

Open <http://127.0.0.1:8876/>. The API is at `/api`; each headless run is saved
under `simulation/runs/`.

```sh
simulation/.venv/bin/python -m unittest discover -s simulation/tests -v
uv run --with playwright python tests/browser_smoke.py
```

The browser smoke covers the real API, M100/Q9 deployment, detailed airframe,
Arthur/Itano portal, robotics dossier, full seven-product production shift,
mass conservation, and post-build robot work proof.
