from __future__ import annotations

from dataclasses import dataclass, asdict
import importlib.util
import math
from typing import Any


@dataclass
class PhysicsResult:
    status: str
    engine: str
    stable: bool | None
    simulated_seconds: float
    contacts: int
    final_body_height_m: float | None
    max_tilt_deg: float | None
    note: str


MJCF = r"""
<mujoco model="q9_contact_validation">
  <compiler angle="degree"/>
  <option timestep="0.002" gravity="0 0 -9.81" integrator="RK4"/>
  <default>
    <geom friction="0.9 0.02 0.001" condim="4"/>
    <joint damping="0.08" armature="0.002"/>
  </default>
  <worldbody>
    <light pos="0 -2 3"/>
    <geom name="floor" type="plane" size="4 4 .1" rgba=".08 .12 .12 1"/>
    <body name="q9" pos="0 0 .29">
      <freejoint name="root"/>
      <inertial pos="0 0 0" mass="0.185" diaginertia="0.00075 0.0011 0.0014"/>
      <geom name="body" type="box" size=".095 .0765 .035" mass="0.168" rgba=".2 .8 .78 1"/>
      <geom name="head" type="box" pos=".087 0 .004" size=".026 .055 .025" mass="0.020" rgba=".95 .45 .35 1"/>
      <body name="fl_hip" pos=".070 .060 -.020"><joint axis="0 1 0" range="-35 35"/><geom type="capsule" fromto="0 0 0 .018 .006 -.080" size=".009" mass=".022"/><body pos=".018 .006 -.080"><joint axis="0 1 0" range="-85 15"/><geom type="capsule" fromto="0 0 0 -.010 0 -.085" size=".008" mass=".018"/><geom name="fl_foot" type="sphere" pos="-.010 0 -.090" size=".014" mass=".004"/></body></body>
      <body name="fr_hip" pos=".070 -.060 -.020"><joint axis="0 1 0" range="-35 35"/><geom type="capsule" fromto="0 0 0 .018 -.006 -.080" size=".009" mass=".022"/><body pos=".018 -.006 -.080"><joint axis="0 1 0" range="-85 15"/><geom type="capsule" fromto="0 0 0 -.010 0 -.085" size=".008" mass=".018"/><geom name="fr_foot" type="sphere" pos="-.010 0 -.090" size=".014" mass=".004"/></body></body>
      <body name="rl_hip" pos="-.070 .060 -.020"><joint axis="0 1 0" range="-35 35"/><geom type="capsule" fromto="0 0 0 -.018 .006 -.080" size=".009" mass=".022"/><body pos="-.018 .006 -.080"><joint axis="0 1 0" range="-85 15"/><geom type="capsule" fromto="0 0 0 .010 0 -.085" size=".008" mass=".018"/><geom name="rl_foot" type="sphere" pos=".010 0 -.090" size=".014" mass=".004"/></body></body>
      <body name="rr_hip" pos="-.070 -.060 -.020"><joint axis="0 1 0" range="-35 35"/><geom type="capsule" fromto="0 0 0 -.018 -.006 -.080" size=".009" mass=".022"/><body pos="-.018 -.006 -.080"><joint axis="0 1 0" range="-85 15"/><geom type="capsule" fromto="0 0 0 .010 0 -.085" size=".008" mass=".018"/><geom name="rr_foot" type="sphere" pos=".010 0 -.090" size=".014" mass=".004"/></body></body>
    </body>
  </worldbody>
</mujoco>
"""


def available() -> bool:
    return importlib.util.find_spec("mujoco") is not None


def validate_q9_contact(simulated_seconds: float = 3.0) -> dict[str, Any]:
    if not available():
        return asdict(PhysicsResult("UNAVAILABLE", "MuJoCo", None, 0.0, 0, None, None, "Install the simulation physics dependency; no result was fabricated."))
    import mujoco

    model = mujoco.MjModel.from_xml_string(MJCF)
    data = mujoco.MjData(model)
    steps = int(simulated_seconds / model.opt.timestep)
    contacts = 0
    max_tilt = 0.0
    for step in range(steps):
        if 350 <= step < 450:
            data.xfrc_applied[1, 1] = 0.42
        else:
            data.xfrc_applied[1, 1] = 0.0
        mujoco.mj_step(model, data)
        contacts = max(contacts, data.ncon)
        quat = data.qpos[3:7]
        w = max(-1.0, min(1.0, float(quat[0])))
        max_tilt = max(max_tilt, math.degrees(2 * math.acos(abs(w))))
    height = float(data.qpos[2])
    stable = height > 0.12 and max_tilt < 45 and contacts >= 4
    return asdict(PhysicsResult(
        "PASS" if stable else "FAIL", f"MuJoCo {mujoco.__version__}", stable,
        simulated_seconds, contacts, round(height, 5), round(max_tilt, 3),
        "Contact-stability smoke test with a declared 0.42 N lateral disturbance; not a deployment certification.",
    ))
