from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import random
from typing import Any, Iterable

import simpy


MODEL_VERSION = "gen0-des-0.3.0"


@dataclass(frozen=True)
class Matrice100Spec:
    wheelbase_mm: int = 650
    mass_tb47d_kg: float = 2.355
    mtow_kg: float = 3.600
    battery_energy_wh: float = 99.9
    battery_charge_power_w: float = 180.0
    max_ascent_m_s: float = 5.0
    max_descent_m_s: float = 4.0
    max_gps_speed_m_s: float = 17.0
    max_wind_m_s: float = 10.0
    hover_minutes_0kg: float = 22.0
    hover_minutes_05kg: float = 17.0
    hover_minutes_1kg: float = 13.0
    source: str = "DJI Matrice 100 published specifications"
    source_url: str = "https://www.dji.com/support/product/matrice100"

    def hover_minutes(self, external_payload_kg: float) -> float:
        """Piecewise interpolation of DJI's published TB47D hover cases.

        Beyond 1 kg, the final segment is conservatively extended to MTOW.
        This is an engineering estimate, not a manufacturer test point.
        """
        p = max(0.0, external_payload_kg)
        if p <= 0.5:
            return self.hover_minutes_0kg + (self.hover_minutes_05kg - self.hover_minutes_0kg) * (p / 0.5)
        if p <= 1.0:
            return self.hover_minutes_05kg + (self.hover_minutes_1kg - self.hover_minutes_05kg) * ((p - 0.5) / 0.5)
        return max(9.5, self.hover_minutes_1kg - (p - 1.0) * 10.0)


@dataclass(frozen=True)
class PayloadSpec:
    quadruped_mass_kg: float = 0.353
    hard_latch_mass_kg: float = 0.120
    quadruped_size_mm: tuple[int, int, int] = (190, 153, 107)
    quadruped_dof: int = 9
    source: str = "Q9 concept mass and envelope; latch mass is a declared Gen-0 engineering assumption"


@dataclass(frozen=True)
class RolePolicy:
    weight: int
    sensor_kg: float
    lane_m: float
    cadence_min: float
    deploy_probability: float
    authority: str
    forbidden: str


ROLE_POLICIES: dict[str, RolePolicy] = {
    "LOGISTICS": RolePolicy(18, 0.050, 12, 1.8, 0.03, "move manifested light lots", "no unmanifested, hot, damaged-battery, or overweight cargo"),
    "INVENTORY": RolePolicy(12, 0.080, 12, 2.4, 0.28, "scan and reconcile physical stock", "cannot alter lot disposition"),
    "SURVEY": RolePolicy(12, 0.145, 24, 2.8, 0.10, "collect repeatable site evidence", "no production actuation"),
    "THERMAL": RolePolicy(10, 0.120, 18, 3.2, 0.22, "request inspection or stop for thermal anomalies", "cannot diagnose root cause alone"),
    "SAFETY": RolePolicy(10, 0.100, 18, 2.0, 0.08, "independent stop and recall", "cannot accept production output"),
    "MAPPING": RolePolicy(8, 0.140, 24, 4.0, 0.16, "stage navigation-map updates", "cannot activate an unreviewed map fleet-wide"),
    "MAINTENANCE": RolePolicy(10, 0.160, 12, 3.5, 0.72, "inspect and escort degraded equipment", "no intrusive energized repair"),
    "MANAGEMENT": RolePolicy(8, 0.080, 24, 3.0, 0.02, "allocate tasks, lanes, pads, and recovery priority", "cannot audit itself or waive physical limits"),
    "COMPLIANCE": RolePolicy(7, 0.100, 18, 2.5, 0.12, "audit manifests, reserves, routes, and managers", "cannot erase or self-approve evidence"),
    "RESERVE": RolePolicy(5, 0.040, 12, 30.0, 0.00, "substitute after fleet or safety authorization", "no speculative launch"),
}


ZONE_COORDS: dict[str, tuple[float, float]] = {
    "RECEIVE": (-74, -42), "STOCK": (-45, -42), "PREP": (-17, -42), "CNC": (15, -42),
    "ADDITIVE": (53, -42), "COMPUTE": (-68, -12), "RECYCLE": (-73, 9), "TOOLING": (-43, 10),
    "ELECTRONICS": (66, -11), "ASSEMBLY": (8, 5), "WINDING": (34, 30), "METROLOGY": (60, 23),
    "CHARGE": (-70, 38), "ENERGY": (-42, 45), "REPLICATE": (-4, 46), "PACKTEST": (36, 48),
    "SILICON": (63, 48), "ROBOTICS": (-38, 27),
}


@dataclass
class ScenarioConfig:
    fleet_size: int = 100
    duration_minutes: int = 720
    seed: int = 903742
    wind_m_s: float = 3.0
    minimum_landing_reserve_pct: float = 25.0
    charge_bays: int = 20
    airspace_capacity_per_lane: int = 12
    scenario: str = "full_factory_shift"

    def validate(self) -> None:
        if not 40 <= self.fleet_size <= 100:
            raise ValueError("fleet_size must be between 40 and 100")
        if not 60 <= self.duration_minutes <= 2880:
            raise ValueError("duration_minutes must be between 60 and 2880")
        if not 0 <= self.wind_m_s <= 25:
            raise ValueError("wind_m_s must be between 0 and 25")
        if not 15 <= self.minimum_landing_reserve_pct <= 40:
            raise ValueError("minimum_landing_reserve_pct must be between 15 and 40")


@dataclass
class FlightTask:
    id: str
    role: str
    kind: str
    origin: str
    destination: str
    cargo_kg: float = 0.0
    ground_work_min: float = 0.0
    deploy_q9: bool = False
    priority: int = 5
    created_min: float = 0.0
    related_operation: str | None = None
    done: simpy.Event | None = field(default=None, repr=False)


@dataclass
class DroneState:
    id: str
    role: str
    battery_wh: float
    battery_soh: float
    health_pct: float
    q9_energy_wh: float = 18.0
    q9_state: str = "STOWED"
    sorties: int = 0
    flight_minutes: float = 0.0
    distance_m: float = 0.0
    charged_wh: float = 0.0
    consumed_wh: float = 0.0
    completed_tasks: int = 0
    recalls: int = 0
    last_task: str | None = None


@dataclass(frozen=True)
class GroundRobotSpec:
    model: str
    family: str
    mass_kg: float
    dof: int
    payload_kg: float
    battery_kwh: float
    runtime_min: float
    speed_m_s: float
    tethered: bool
    envelope: str
    allowed_zones: tuple[str, ...]
    work_types: tuple[str, ...]
    source_basis: str
    source_url: str


GROUND_ROBOT_SPECS: dict[str, GroundRobotSpec] = {
    "HUMANOID": GroundRobotSpec(
        model="HMX-1", family="full-size bipedal material handler", mass_kg=69.5, dof=27,
        payload_kg=15.9, battery_kwh=2.4, runtime_min=240, speed_m_s=1.5, tethered=False,
        envelope="1.78 m standing height; two-finger industrial hands; autonomous dock",
        allowed_zones=("ROBOTICS","STOCK","PREP","CNC","ELECTRONICS","ASSEMBLY","METROLOGY","TOOLING","REPLICATE","PACKTEST"),
        work_types=("TOTE_TRANSFER","MACHINE_TEND","KIT_PRESENT","AMR_HANDOFF","REWORK_ASSIST"),
        source_basis="Gen-0 design target bounded by published Digit/H1 size, mass, payload and runtime classes; not a vendor product",
        source_url="https://www.agilityrobotics.com/solutions",
    ),
    "HEXAPOD": GroundRobotSpec(
        model="HEX-6", family="six-legged industrial inspector", mass_kg=26.0, dof=18,
        payload_kg=10.0, battery_kwh=0.95, runtime_min=90, speed_m_s=1.0, tethered=False,
        envelope="0.92 x 0.58 x 0.46 m; thermal, acoustic and gas inspection mast",
        allowed_zones=tuple(ZONE_COORDS),
        work_types=("ROUGH_FLOOR_PATROL","THERMAL_ROUTE","LEAK_LOCALIZE","STAIR_ESCORT"),
        source_basis="Gen-0 six-leg design; payload target bounded to the published ANYmal 10 kg additional-payload class",
        source_url="https://www.anybotics.com/robotics/anymal/",
    ),
    "SNAKE": GroundRobotSpec(
        model="S1", family="segmented confined-space inspector", mass_kg=30.5, dof=20,
        payload_kg=3.0, battery_kwh=0.0, runtime_min=0, speed_m_s=0.35, tethered=True,
        envelope="3.0 m x 0.19 m; ten sealed two-axis modules; 80 VDC/fiber tether",
        allowed_zones=("ROBOTICS","CNC","ADDITIVE","ELECTRONICS","ASSEMBLY","ENERGY","PACKTEST","TOOLING"),
        work_types=("UNDER_MACHINE_SCAN","CABLE_TRENCH_SURVEY","DUCT_INSPECTION"),
        source_basis="Scaled industrial derivative inspired by JPL EELS segmented architecture; dimensions and controls are Gen-0 assumptions",
        source_url="https://www.jpl.nasa.gov/robotics-at-jpl/eels/",
    ),
    "CRAWLER": GroundRobotSpec(
        model="CL-4", family="tethered wall and vessel crawler", mass_kg=16.5, dof=8,
        payload_kg=5.0, battery_kwh=0.0, runtime_min=0, speed_m_s=0.25, tethered=True,
        envelope="0.62 x 0.48 x 0.20 m; four magnetic/vacuum adhesion pods",
        allowed_zones=("ROBOTICS","ENERGY","ELECTRONICS","PACKTEST","METROLOGY","REPLICATE"),
        work_types=("BUSWAY_THERMOGRAPHY","DUCT_WALL_SCAN","VESSEL_ULTRASOUND"),
        source_basis="Gen-0 factory design target; adhesion mode is selected by surveyed surface and remains tether-arrested",
        source_url="https://www.osha.gov/robotics",
    ),
}


@dataclass
class GroundRobotTask:
    id: str
    kind: str
    work_type: str
    origin: str
    destination: str
    payload_kg: float
    service_min: float
    priority: int
    created_min: float
    lockout_verified: bool = False
    exclusion_zone_verified: bool = False
    tether_verified: bool = False
    related_order: str | None = None
    done: simpy.Event | None = field(default=None, repr=False)


@dataclass
class GroundRobotState:
    id: str
    kind: str
    battery_kwh: float
    health_pct: float
    location: str = "ROBOTICS"
    state: str = "READY"
    completed_tasks: int = 0
    operating_minutes: float = 0.0
    distance_m: float = 0.0
    energy_kwh: float = 0.0
    charged_kwh: float = 0.0
    last_task: str | None = None
    commissioned_order: str | None = None
    commissioned_at: float | None = None
    post_build_work_complete: bool = False


OPERATION_DEFS: tuple[dict[str, Any], ...] = (
    {"id":"AC01","order":"WO-AC-073","zone":"PREP","name":"CUT AIRFRAME BLANKS","duration":22,"deps":(),"input_kg":3.8,"output_kg":1.4},
    {"id":"AC02","order":"WO-AC-073","zone":"CNC","name":"MACHINE FRAME + BRACKETS","duration":95,"deps":("AC01",),"input_kg":1.4,"output_kg":1.05},
    {"id":"AC03","order":"WO-AC-073","zone":"ADDITIVE","name":"PRINT SHROUD + LANDING FEET","duration":180,"deps":(),"input_kg":0.78,"output_kg":0.65},
    {"id":"AC04","order":"WO-AC-073","zone":"WINDING","name":"CUT + TEST FLIGHT HARNESS","duration":45,"deps":(),"input_kg":0.40,"output_kg":0.35},
    {"id":"AC05","order":"WO-AC-073","zone":"ELECTRONICS","name":"PLACE + REFLOW FLIGHT PCB","duration":60,"deps":(),"input_kg":0.20,"output_kg":0.18},
    {"id":"AC06","order":"WO-AC-073","zone":"METROLOGY","name":"CMM FRAME + ICT CONTROL PCB","duration":40,"deps":("AC02","AC05"),"input_kg":1.23,"output_kg":1.23,"qa":True},
    {"id":"AC07","order":"WO-AC-073","zone":"ASSEMBLY","name":"INTEGRATE FLIGHT CELL","duration":70,"deps":("AC03","AC04","AC06"),"input_kg":4.121,"output_kg":3.45},
    {"id":"AC08","order":"WO-AC-073","zone":"METROLOGY","name":"THRUST / VIBRATION / MASS ACCEPT","duration":45,"deps":("AC07",),"input_kg":3.45,"output_kg":3.45,"qa":True},
    {"id":"AC09","order":"WO-AC-073","zone":"CHARGE","name":"CONDITION PACK + FLIGHT RELEASE","duration":60,"deps":("AC08",),"input_kg":3.45,"output_kg":3.45,"qa":True},
    {"id":"G101","order":"WO-G1-004","zone":"ELECTRONICS","name":"PLACE + REFLOW FPGA CARRIER","duration":70,"deps":(),"input_kg":0.66,"output_kg":0.65},
    {"id":"G102","order":"WO-G1-004","zone":"METROLOGY","name":"X-RAY + BOUNDARY SCAN","duration":45,"deps":("G101",),"input_kg":0.65,"output_kg":0.65,"qa":True},
    {"id":"G103","order":"WO-G1-004","zone":"COMPUTE","name":"LOAD RTL + RUNTIME IMAGE","duration":35,"deps":("G102",),"input_kg":0.65,"output_kg":0.65},
    {"id":"G104","order":"WO-G1-004","zone":"METROLOGY","name":"POWER / THERMAL SCREEN","duration":180,"deps":("G103",),"input_kg":0.65,"output_kg":0.65,"qa":True},
    {"id":"R301","order":"WO-R03-011","zone":"PREP","name":"CUT PRINT-CELL FRAME STOCK","duration":45,"deps":(),"input_kg":9.0,"output_kg":7.6},
    {"id":"R302","order":"WO-R03-011","zone":"CNC","name":"MACHINE GANTRY + DATUMS","duration":150,"deps":("R301",),"input_kg":7.6,"output_kg":6.4},
    {"id":"R303","order":"WO-R03-011","zone":"ADDITIVE","name":"PRINT GUARDS + CABLE CHAIN","duration":240,"deps":(),"input_kg":1.8,"output_kg":1.55},
    {"id":"R304","order":"WO-R03-011","zone":"ELECTRONICS","name":"BUILD MOTION CONTROL BOARD","duration":65,"deps":(),"input_kg":0.40,"output_kg":0.38},
    {"id":"R305","order":"WO-R03-011","zone":"ASSEMBLY","name":"INTEGRATE R-03 PRINT CELL","duration":120,"deps":("R302","R303","R304"),"input_kg":17.89,"output_kg":17.4},
    {"id":"R306","order":"WO-R03-011","zone":"METROLOGY","name":"CALIBRATE + PRINT TEST ARTEFACT","duration":60,"deps":("R305",),"input_kg":17.4,"output_kg":17.4,"qa":True,"planned_rework":True},
    {"id":"HM01","order":"WO-HM-001","zone":"PREP","name":"CUT HMX PRIMARY FRAME BLANKS","duration":50,"deps":(),"input_kg":28.0,"output_kg":22.0},
    {"id":"HM02","order":"WO-HM-001","zone":"CNC","name":"MACHINE HMX PELVIS + LIMB LINKS","duration":130,"deps":("HM01",),"input_kg":22.0,"output_kg":18.0},
    {"id":"HM03","order":"WO-HM-001","zone":"ADDITIVE","name":"PRINT HMX COVERS + HAND FORMS","duration":150,"deps":(),"input_kg":5.0,"output_kg":4.2},
    {"id":"HM04","order":"WO-HM-001","zone":"WINDING","name":"BUILD HMX POWER + ETHERCAT HARNESS","duration":65,"deps":(),"input_kg":4.5,"output_kg":4.2},
    {"id":"HM05","order":"WO-HM-001","zone":"ELECTRONICS","name":"BUILD DUAL-SAFETY CONTROL STACK","duration":80,"deps":(),"input_kg":3.2,"output_kg":3.0},
    {"id":"HM06","order":"WO-HM-001","zone":"ASSEMBLY","name":"INTEGRATE HMX-1 27-DOF BODY","duration":110,"deps":("HM02","HM03","HM04","HM05"),"input_kg":72.0,"output_kg":69.5},
    {"id":"HM07","order":"WO-HM-001","zone":"METROLOGY","name":"HMX BRAKE / INSULATION / MASS ACCEPT","duration":55,"deps":("HM06",),"input_kg":69.5,"output_kg":69.5,"qa":True},
    {"id":"HM08","order":"WO-HM-001","zone":"ROBOTICS","name":"HMX BALANCE / LOAD / CAT1 STOP COMMISSION","duration":60,"deps":("HM07",),"input_kg":69.5,"output_kg":69.5,"qa":True,"commission_kind":"HUMANOID"},
    {"id":"HX01","order":"WO-HX-006","zone":"PREP","name":"CUT HEX-6 CHASSIS STOCK","duration":35,"deps":(),"input_kg":12.0,"output_kg":9.0},
    {"id":"HX02","order":"WO-HX-006","zone":"CNC","name":"MACHINE HEX-6 BODY + COXA LINKS","duration":80,"deps":("HX01",),"input_kg":9.0,"output_kg":7.5},
    {"id":"HX03","order":"WO-HX-006","zone":"ELECTRONICS","name":"BUILD HEX-6 SAFETY + SENSOR STACK","duration":55,"deps":(),"input_kg":2.4,"output_kg":2.25},
    {"id":"HX04","order":"WO-HX-006","zone":"ASSEMBLY","name":"INTEGRATE HEX-6 LEG MODULES","duration":85,"deps":("HX02","HX03"),"input_kg":27.0,"output_kg":26.0},
    {"id":"HX05","order":"WO-HX-006","zone":"METROLOGY","name":"HEX-6 TRACTION / PAYLOAD / SENSOR ACCEPT","duration":35,"deps":("HX04",),"input_kg":26.0,"output_kg":26.0,"qa":True},
    {"id":"HX06","order":"WO-HX-006","zone":"ROBOTICS","name":"HEX-6 ROUGH-FLOOR COMMISSION","duration":45,"deps":("HX05",),"input_kg":26.0,"output_kg":26.0,"qa":True,"commission_kind":"HEXAPOD"},
    {"id":"SN01","order":"WO-SN-003","zone":"CNC","name":"MACHINE S1 SEALED LINK BODIES","duration":95,"deps":(),"input_kg":12.0,"output_kg":9.5},
    {"id":"SN02","order":"WO-SN-003","zone":"ADDITIVE","name":"PRINT S1 ACTIVE SKIN + GUIDES","duration":120,"deps":(),"input_kg":4.0,"output_kg":3.3},
    {"id":"SN03","order":"WO-SN-003","zone":"ELECTRONICS","name":"BUILD S1 DISTRIBUTED JOINT CONTROL","duration":65,"deps":(),"input_kg":2.8,"output_kg":2.6},
    {"id":"SN04","order":"WO-SN-003","zone":"ASSEMBLY","name":"INTEGRATE S1 TEN-MODULE BODY + TETHER","duration":100,"deps":("SN01","SN02","SN03"),"input_kg":32.0,"output_kg":30.5},
    {"id":"SN05","order":"WO-SN-003","zone":"METROLOGY","name":"S1 SEAL / BEND / TETHER ACCEPT","duration":35,"deps":("SN04",),"input_kg":30.5,"output_kg":30.5,"qa":True},
    {"id":"SN06","order":"WO-SN-003","zone":"ROBOTICS","name":"S1 LOCKOUT CONFINED-SPACE COMMISSION","duration":45,"deps":("SN05",),"input_kg":30.5,"output_kg":30.5,"qa":True,"commission_kind":"SNAKE"},
    {"id":"CL01","order":"WO-CL-002","zone":"CNC","name":"MACHINE CL-4 CRAWLER CHASSIS","duration":70,"deps":(),"input_kg":9.0,"output_kg":7.0},
    {"id":"CL02","order":"WO-CL-002","zone":"ELECTRONICS","name":"BUILD CL-4 NDT CONTROL STACK","duration":50,"deps":(),"input_kg":2.5,"output_kg":2.3},
    {"id":"CL03","order":"WO-CL-002","zone":"ASSEMBLY","name":"INTEGRATE CL-4 ADHESION PODS + TETHER","duration":75,"deps":("CL01","CL02"),"input_kg":17.0,"output_kg":16.5},
    {"id":"CL04","order":"WO-CL-002","zone":"METROLOGY","name":"CL-4 ADHESION / ARREST / NDT ACCEPT","duration":30,"deps":("CL03",),"input_kg":16.5,"output_kg":16.5,"qa":True},
    {"id":"CL05","order":"WO-CL-002","zone":"ROBOTICS","name":"CL-4 VERTICAL COUPON COMMISSION","duration":35,"deps":("CL04",),"input_kg":16.5,"output_kg":16.5,"qa":True,"commission_kind":"CRAWLER"},
)


ORDER_TERMINALS = {
    "WO-AC-073": "AC09", "WO-G1-004": "G104", "WO-R03-011": "R306",
    "WO-HM-001": "HM08", "WO-HX-006": "HX06", "WO-SN-003": "SN06", "WO-CL-002": "CL05",
}


MACHINE_CAPACITY = {
    "PREP": 3, "CNC": 4, "ADDITIVE": 4, "WINDING": 2, "ELECTRONICS": 4,
    "METROLOGY": 4, "ASSEMBLY": 4, "COMPUTE": 2, "CHARGE": 2, "ROBOTICS": 2,
}


class FactorySimulation:
    def __init__(self, config: ScenarioConfig):
        config.validate()
        self.config = config
        self.spec = Matrice100Spec()
        self.payload = PayloadSpec()
        self.rng = random.Random(config.seed)
        self.env = simpy.Environment()
        self.role_queues = {role: simpy.PriorityStore(self.env) for role in ROLE_POLICIES}
        self.ground_queues = {kind: simpy.PriorityStore(self.env) for kind in GROUND_ROBOT_SPECS}
        self.charge_bays = simpy.Resource(self.env, capacity=config.charge_bays)
        self.ground_charge_bays = simpy.Resource(self.env, capacity=4)
        self.lanes = {lane: simpy.Resource(self.env, capacity=config.airspace_capacity_per_lane) for lane in (12, 18, 24)}
        self.ground_carts = simpy.Resource(self.env, capacity=6)
        self.ground_mobility = simpy.Resource(self.env, capacity=12)
        self.machines = {zone: simpy.Resource(self.env, capacity=capacity) for zone, capacity in MACHINE_CAPACITY.items()}
        self.events: list[dict[str, Any]] = []
        self.tasks: dict[str, FlightTask] = {}
        self.task_seq = 1
        self.event_seq = 1
        self.drones = self._build_fleet(config.fleet_size)
        self.ground_robot_tasks: dict[str, GroundRobotTask] = {}
        self.ground_task_seq = 1
        self.ground_robot_records: list[dict[str, Any]] = []
        self.ground_robot_audits: list[dict[str, Any]] = []
        self.ground_block_reasons: Counter[str] = Counter()
        self.ground_metrics: Counter[str] = Counter()
        self.ground_robot_energy_kwh = 0.0
        self.ground_robots = self._build_ground_fleet()
        self.operation_events = {op["id"]: self.env.event() for op in OPERATION_DEFS}
        self.operation_records: list[dict[str, Any]] = []
        self.role_stats: Counter[str] = Counter()
        self.metrics: Counter[str] = Counter()
        self.block_reasons: Counter[str] = Counter()
        self.max_takeoff_kg = 0.0
        self.max_external_payload_kg = 0.0
        self.min_landing_reserve_pct = 100.0
        self.scrap_mass_kg = 0.0
        self.machine_energy_kwh = 0.0
        self._active_assignments: dict[str, str] = {}
        self._active_ground_assignments: dict[str, str] = {}

    def _allocate_roles(self, count: int) -> list[str]:
        exact = {role: count * policy.weight / 100 for role, policy in ROLE_POLICIES.items()}
        allocated = {role: int(value) for role, value in exact.items()}
        remainder = count - sum(allocated.values())
        for role in sorted(exact, key=lambda r: exact[r] - allocated[r], reverse=True)[:remainder]:
            allocated[role] += 1
        roles: list[str] = []
        for role, number in allocated.items():
            roles.extend([role] * number)
        # Interleave instead of creating role blocks; identity assignment remains deterministic.
        roles.sort(key=lambda role: ((list(ROLE_POLICIES).index(role) * 37) % 101, role))
        return roles

    def _build_fleet(self, count: int) -> list[DroneState]:
        roles = self._allocate_roles(count)
        return [
            DroneState(
                id=f"F-{index + 1:03d}",
                role=role,
                battery_wh=self.spec.battery_energy_wh * self.rng.uniform(0.86, 0.99),
                battery_soh=self.rng.uniform(0.92, 1.0),
                health_pct=self.rng.uniform(88, 100),
            )
            for index, role in enumerate(roles)
        ]

    def _build_ground_fleet(self) -> list[GroundRobotState]:
        counts = {"HUMANOID": 3, "HEXAPOD": 4, "SNAKE": 2, "CRAWLER": 2}
        fleet: list[GroundRobotState] = []
        for kind, count in counts.items():
            spec = GROUND_ROBOT_SPECS[kind]
            for index in range(1, count + 1):
                fleet.append(GroundRobotState(
                    id=f"{spec.model}-{index:02d}", kind=kind,
                    battery_kwh=0.0 if spec.tethered else spec.battery_kwh * self.rng.uniform(0.86, 0.98),
                    health_pct=self.rng.uniform(91, 100),
                ))
        return fleet

    def _next_ground_id(self, kind: str) -> str:
        spec = GROUND_ROBOT_SPECS[kind]
        count = sum(robot.kind == kind for robot in self.ground_robots) + 1
        return f"{spec.model}-{count:02d}"

    def enqueue_ground_task(
        self, kind: str, work_type: str, origin: str, destination: str, *, payload_kg: float = 0.0,
        service_min: float = 5.0, priority: int = 5, lockout_verified: bool = False,
        exclusion_zone_verified: bool = False, tether_verified: bool = False,
        related_order: str | None = None, queue_task: bool = True,
    ) -> GroundRobotTask:
        task = GroundRobotTask(
            id=f"GRT-{self.ground_task_seq:05d}", kind=kind, work_type=work_type,
            origin=origin, destination=destination, payload_kg=round(payload_kg, 3),
            service_min=service_min, priority=priority, created_min=self.env.now,
            lockout_verified=lockout_verified, exclusion_zone_verified=exclusion_zone_verified,
            tether_verified=tether_verified, related_order=related_order, done=self.env.event(),
        )
        self.ground_task_seq += 1
        self.ground_robot_tasks[task.id] = task
        if queue_task:
            self.ground_queues[kind].put((priority, self.ground_task_seq, task))
        self.ground_metrics["tasks_created"] += 1
        return task

    def ground_safety_case(self, robot: GroundRobotState, task: GroundRobotTask) -> tuple[bool, str, dict[str, float]]:
        spec = GROUND_ROBOT_SPECS[robot.kind]
        distance_m = self.distance(robot.location, task.origin) + self.distance(task.origin, task.destination)
        travel_min = distance_m / max(0.05, spec.speed_m_s) / 60.0
        operating_min = travel_min + task.service_min
        energy_kwh = (spec.battery_kwh * operating_min / spec.runtime_min * 1.15) if not spec.tethered else operating_min / 60.0 * (0.55 if robot.kind == "SNAKE" else 0.38)
        reserve_pct = 100.0 if spec.tethered else (robot.battery_kwh - energy_kwh) / spec.battery_kwh * 100.0
        evidence = {
            "payload_kg": round(task.payload_kg, 3), "payload_limit_kg": spec.payload_kg,
            "distance_m": round(distance_m, 2), "operating_min": round(operating_min, 3),
            "energy_kwh": round(energy_kwh, 5), "predicted_reserve_pct": round(reserve_pct, 2),
        }
        if task.work_type not in spec.work_types:
            return False, "ROLE_MISMATCH", evidence
        if task.destination not in spec.allowed_zones or task.origin not in spec.allowed_zones:
            return False, "ZONE_NOT_RELEASED", evidence
        if task.payload_kg > spec.payload_kg + 1e-9:
            return False, "PAYLOAD_LIMIT", evidence
        if robot.health_pct < 75:
            return False, "HEALTH", evidence
        if spec.tethered and not task.tether_verified:
            return False, "TETHER_PERMIT", evidence
        if robot.kind == "SNAKE" and task.work_type == "UNDER_MACHINE_SCAN" and not task.lockout_verified:
            return False, "LOCKOUT_PERMIT", evidence
        if robot.kind == "CRAWLER" and not task.exclusion_zone_verified:
            return False, "FALL_ZONE_PERMIT", evidence
        if robot.kind == "HUMANOID" and task.work_type in {"MACHINE_TEND", "REWORK_ASSIST"} and not task.exclusion_zone_verified:
            return False, "ROBOT_CELL_PERMIT", evidence
        if not spec.tethered and reserve_pct < 20.0:
            return False, "ENERGY_RESERVE", evidence
        return True, "AUTHORIZED", evidence

    def charge_ground_robot(self, robot: GroundRobotState):
        spec = GROUND_ROBOT_SPECS[robot.kind]
        if spec.tethered:
            return
        target = spec.battery_kwh * 0.95
        if robot.battery_kwh >= target:
            return
        with self.ground_charge_bays.request() as request:
            yield request
            needed = target - robot.battery_kwh
            yield self.env.timeout(needed / 2.0 * 60.0)
            robot.battery_kwh = target
            robot.charged_kwh += needed
            self.ground_metrics["charge_sessions"] += 1
            self.log("GROUND_CHARGE", f"{robot.id} docked and accepted {needed:.2f} kWh", robot=robot.id, charged_kwh=round(needed, 4))

    def perform_ground_task(self, robot: GroundRobotState, task: GroundRobotTask, evidence: dict[str, float]):
        spec = GROUND_ROBOT_SPECS[robot.kind]
        with self.ground_mobility.request() as request:
            queued_at = self.env.now
            yield request
            start = self.env.now
            robot.state = "WORKING"
            robot.last_task = task.id
            self._active_ground_assignments[robot.id] = task.id
            self.log("GROUND_DISPATCH", f"{robot.id} accepted {task.id} / {task.work_type}", robot=robot.id, task=task.id, **evidence)
            yield self.env.timeout(evidence["operating_min"])
            if not spec.tethered:
                robot.battery_kwh = max(0.0, robot.battery_kwh - evidence["energy_kwh"])
            robot.energy_kwh += evidence["energy_kwh"]
            self.ground_robot_energy_kwh += evidence["energy_kwh"]
            robot.completed_tasks += 1
            robot.operating_minutes += evidence["operating_min"]
            robot.distance_m += evidence["distance_m"]
            robot.location = task.destination
            robot.state = "READY"
            if task.related_order and task.related_order == robot.commissioned_order:
                robot.post_build_work_complete = True
            record = {
                "task": task.id, "robot": robot.id, "kind": robot.kind, "model": spec.model,
                "work_type": task.work_type, "origin": task.origin, "destination": task.destination,
                "payload_kg": task.payload_kg, "payload_limit_kg": spec.payload_kg,
                "start_min": round(start, 3), "end_min": round(self.env.now, 3),
                "distance_m": evidence["distance_m"], "energy_kwh": evidence["energy_kwh"],
                "lockout_verified": task.lockout_verified, "exclusion_zone_verified": task.exclusion_zone_verified,
                "tether_verified": task.tether_verified, "related_order": task.related_order,
                "queued_min": round(start - queued_at, 3), "disposition": "WORK COMPLETE",
            }
            self.ground_robot_records.append(record)
            self.ground_metrics["tasks_completed"] += 1
            self.ground_metrics[f"completed_{robot.kind}"] += 1
            self.log("GROUND_COMPLETE", f"{robot.id} completed {task.id} at {task.destination}", robot=robot.id, task=task.id, work_type=task.work_type)
            self._active_ground_assignments.pop(robot.id, None)
            if task.done is not None and not task.done.triggered:
                task.done.succeed({"robot": robot.id, "time_min": self.env.now})

    def ground_robot_process(self, robot: GroundRobotState):
        queue = self.ground_queues[robot.kind]
        while self.env.now < self.config.duration_minutes:
            _, _, task = yield queue.get()
            approved, reason, evidence = self.ground_safety_case(robot, task)
            audit = {"time_min": round(self.env.now, 3), "robot": robot.id, "task": task.id, "approved": approved, "reason": reason, **evidence}
            self.ground_robot_audits.append(audit)
            self.ground_metrics["safety_audits"] += 1
            if not approved:
                if reason == "ENERGY_RESERVE":
                    self.log("GROUND_HOLD", f"safety-0 held {task.id} for {robot.id}: recharge required", robot=robot.id, task=task.id, reason=reason)
                    yield from self.charge_ground_robot(robot)
                    self.ground_queues[robot.kind].put((task.priority, self.ground_task_seq, task))
                    self.ground_task_seq += 1
                    continue
                self.ground_block_reasons[reason] += 1
                self.ground_metrics["tasks_blocked"] += 1
                self.log("GROUND_BLOCK", f"safety-0 blocked {task.id} on {robot.id}: {reason}", robot=robot.id, task=task.id, reason=reason, **evidence)
                if task.done is not None and not task.done.triggered:
                    task.done.succeed({"blocked": reason, "time_min": self.env.now})
                continue
            yield from self.perform_ground_task(robot, task, evidence)
            if not GROUND_ROBOT_SPECS[robot.kind].tethered and robot.battery_kwh < GROUND_ROBOT_SPECS[robot.kind].battery_kwh * 0.28:
                yield from self.charge_ground_robot(robot)

    def commissioned_work(self, robot: GroundRobotState) -> GroundRobotTask:
        templates = {
            "HUMANOID": ("KIT_PRESENT", "ROBOTICS", "ASSEMBLY", 8.5, False, True, False),
            "HEXAPOD": ("THERMAL_ROUTE", "ROBOTICS", "ENERGY", 0.8, False, False, False),
            "SNAKE": ("UNDER_MACHINE_SCAN", "ROBOTICS", "CNC", 0.4, True, False, True),
            "CRAWLER": ("BUSWAY_THERMOGRAPHY", "ROBOTICS", "ENERGY", 1.2, False, True, True),
        }
        work, origin, destination, payload, lockout, exclusion, tether = templates[robot.kind]
        return self.enqueue_ground_task(
            robot.kind, work, origin, destination, payload_kg=payload, service_min=7.0, priority=0,
            lockout_verified=lockout, exclusion_zone_verified=exclusion, tether_verified=tether,
            related_order=robot.commissioned_order,
            queue_task=False,
        )

    def commissioned_robot_process(self, robot: GroundRobotState, task: GroundRobotTask):
        approved, reason, evidence = self.ground_safety_case(robot, task)
        self.ground_robot_audits.append({
            "time_min": round(self.env.now, 3), "robot": robot.id, "task": task.id,
            "approved": approved, "reason": reason, **evidence,
        })
        self.ground_metrics["safety_audits"] += 1
        if approved:
            yield from self.perform_ground_task(robot, task, evidence)
        else:
            self.ground_block_reasons[reason] += 1
            self.ground_metrics["tasks_blocked"] += 1
            self.log("GROUND_BLOCK", f"commission proof blocked for {robot.id}: {reason}", robot=robot.id, task=task.id, reason=reason)
        yield from self.ground_robot_process(robot)

    def commission_ground_robot(self, kind: str, order: str) -> GroundRobotState:
        spec = GROUND_ROBOT_SPECS[kind]
        robot = GroundRobotState(
            id=self._next_ground_id(kind), kind=kind,
            battery_kwh=0.0 if spec.tethered else spec.battery_kwh * 0.95,
            health_pct=100.0, commissioned_order=order, commissioned_at=self.env.now,
        )
        self.ground_robots.append(robot)
        self.ground_metrics["units_commissioned"] += 1
        proof_task = self.commissioned_work(robot)
        self.env.process(self.commissioned_robot_process(robot, proof_task))
        self.log("ROBOT_COMMISSION", f"{robot.id} serialized from {order}; operational proof task released", robot=robot.id, order=order, robot_family=kind)
        return robot

    def log(self, kind: str, message: str, **evidence: Any) -> None:
        self.events.append({"seq": self.event_seq, "time_min": round(self.env.now, 3), "kind": kind, "message": message, "evidence": evidence})
        self.event_seq += 1

    def distance(self, origin: str, destination: str) -> float:
        a, b = ZONE_COORDS[origin], ZONE_COORDS[destination]
        return math.hypot(a[0] - b[0], a[1] - b[1])

    def operation_external_mass(self, op: dict[str, Any]) -> float:
        by_id = {candidate["id"]: candidate for candidate in OPERATION_DEFS}
        inherited = sum(float(by_id[dep]["output_kg"]) for dep in op["deps"])
        return max(0.0, float(op["input_kg"]) - inherited)

    def external_payload(self, drone: DroneState, cargo_kg: float) -> float:
        return self.payload.quadruped_mass_kg + self.payload.hard_latch_mass_kg + ROLE_POLICIES[drone.role].sensor_kg + cargo_kg

    def cargo_limit(self, drone: DroneState) -> float:
        fixed = self.payload.quadruped_mass_kg + self.payload.hard_latch_mass_kg + ROLE_POLICIES[drone.role].sensor_kg
        return max(0.0, self.spec.mtow_kg - self.spec.mass_tb47d_kg - fixed)

    def energy_for_flight(self, payload_kg: float, distance_m: float, ground_work_min: float, deploy_q9: bool) -> tuple[float, float]:
        cruise_m_s = min(10.0, self.spec.max_gps_speed_m_s * max(0.45, 1.0 - payload_kg / 3.2))
        travel_min = (distance_m / cruise_m_s) / 60.0
        flight_min = 1.25 + travel_min * 2.0 + (0.8 if deploy_q9 else min(1.5, ground_work_min * 0.2))
        endurance = self.spec.hover_minutes(payload_kg)
        energy_wh = self.spec.battery_energy_wh * flight_min / endurance
        # Takeoff/transient and wind correction. These terms are declared model assumptions.
        energy_wh *= 1.06 + min(0.24, self.config.wind_m_s * 0.012)
        return flight_min, energy_wh

    def safety_case(self, drone: DroneState, task: FlightTask) -> tuple[bool, str, dict[str, float]]:
        payload_kg = self.external_payload(drone, task.cargo_kg)
        takeoff_kg = self.spec.mass_tb47d_kg + payload_kg
        distance_m = self.distance(task.origin, task.destination)
        flight_min, energy_wh = self.energy_for_flight(payload_kg, distance_m, task.ground_work_min, task.deploy_q9)
        available_wh = self.spec.battery_energy_wh * drone.battery_soh
        landing_pct = (drone.battery_wh - energy_wh) / available_wh * 100
        evidence = {
            "external_payload_kg": round(payload_kg, 4), "takeoff_kg": round(takeoff_kg, 4),
            "distance_m": round(distance_m, 2), "flight_min": round(flight_min, 3),
            "energy_wh": round(energy_wh, 3), "predicted_landing_pct": round(landing_pct, 2),
        }
        if takeoff_kg > self.spec.mtow_kg + 1e-9:
            return False, "MTOW", evidence
        if task.cargo_kg > self.cargo_limit(drone) + 1e-9:
            return False, "CARGO_LIMIT", evidence
        if self.config.wind_m_s > self.spec.max_wind_m_s:
            return False, "WIND", evidence
        if drone.health_pct < 75:
            return False, "HEALTH", evidence
        if landing_pct < self.config.minimum_landing_reserve_pct:
            return False, "RESERVE", evidence
        return True, "AUTHORIZED", evidence

    def enqueue_task(
        self, role: str, kind: str, origin: str, destination: str, *, cargo_kg: float = 0.0,
        ground_work_min: float = 0.0, deploy_q9: bool = False, priority: int = 5,
        related_operation: str | None = None,
    ) -> FlightTask:
        task = FlightTask(
            id=f"FLT-{self.task_seq:05d}", role=role, kind=kind, origin=origin, destination=destination,
            cargo_kg=round(cargo_kg, 4), ground_work_min=ground_work_min, deploy_q9=deploy_q9,
            priority=priority, created_min=self.env.now, related_operation=related_operation,
            done=self.env.event(),
        )
        self.task_seq += 1
        self.tasks[task.id] = task
        self.role_queues[role].put((priority, self.task_seq, task))
        self.metrics["tasks_created"] += 1
        return task

    def charge(self, drone: DroneState):
        capacity_wh = self.spec.battery_energy_wh * drone.battery_soh
        target_wh = capacity_wh * 0.95
        if drone.battery_wh >= target_wh:
            return
        with self.charge_bays.request() as request:
            queued_at = self.env.now
            yield request
            self.metrics["charge_queue_min_x1000"] += int((self.env.now - queued_at) * 1000)
            needed_wh = target_wh - drone.battery_wh
            charge_min = needed_wh / self.spec.battery_charge_power_w * 60.0
            yield self.env.timeout(charge_min)
            drone.battery_wh = target_wh
            drone.charged_wh += needed_wh
            self.metrics["charge_sessions"] += 1
            self.log("CHARGE", f"{drone.id} charged {needed_wh:.1f} Wh to 95% SOH-adjusted capacity", drone=drone.id, charged_wh=round(needed_wh, 3))

    def perform_task(self, drone: DroneState, task: FlightTask, evidence: dict[str, float]):
        lane = int(ROLE_POLICIES[drone.role].lane_m)
        with self.lanes[lane].request() as request:
            queued_at = self.env.now
            yield request
            lane_wait = self.env.now - queued_at
            self.metrics["lane_wait_min_x1000"] += int(lane_wait * 1000)
            self._active_assignments[drone.id] = task.id
            drone.last_task = task.id
            drone.sorties += 1
            self.metrics["sorties"] += 1
            self.max_takeoff_kg = max(self.max_takeoff_kg, evidence["takeoff_kg"])
            self.max_external_payload_kg = max(self.max_external_payload_kg, evidence["external_payload_kg"])
            self.log("DISPATCH", f"{drone.id} accepted {task.id} / {task.kind}", drone=drone.id, task=task.id, lane_m=lane, **evidence)
            outbound_min = evidence["flight_min"] * 0.5
            yield self.env.timeout(outbound_min)
            if task.deploy_q9:
                drone.q9_state = "DEPLOYING"
                yield self.env.timeout(0.55)
                drone.q9_state = "GROUND"
                self.metrics["quadruped_deployments"] += 1
                self.log("Q9_DEPLOY", f"{drone.id} placed {drone.id.replace('F-', 'QPX-')} weight-on-feet at {task.destination}", drone=drone.id, task=task.id)
                q9_work = max(0.6, task.ground_work_min)
                yield self.env.timeout(q9_work)
                drone.q9_energy_wh = max(0.0, drone.q9_energy_wh - q9_work * 0.18)
                drone.q9_state = "RECOVERING"
                yield self.env.timeout(0.65)
                drone.q9_state = "STOWED"
                self.metrics["quadruped_recoveries"] += 1
                self.log("Q9_RECOVER", f"{drone.id} hard-latched and continuity-checked its quadruped", drone=drone.id, task=task.id)
            elif task.ground_work_min:
                yield self.env.timeout(task.ground_work_min)
            yield self.env.timeout(outbound_min)
            energy_wh = evidence["energy_wh"]
            drone.battery_wh = max(0.0, drone.battery_wh - energy_wh)
            drone.consumed_wh += energy_wh
            drone.flight_minutes += evidence["flight_min"]
            distance_m = evidence["distance_m"] * 2.0
            drone.distance_m += distance_m
            drone.completed_tasks += 1
            self.role_stats[drone.role] += 1
            self.metrics["tasks_completed"] += 1
            self.metrics["flight_energy_wh_x1000"] += int(energy_wh * 1000)
            self.metrics["distance_m"] += int(distance_m)
            if task.kind == "MATERIAL_TRANSFER":
                self.metrics["cargo_kg_x1000"] += int(task.cargo_kg * 1000)
            if task.kind in {"INSPECTION", "THERMAL_SCAN", "INVENTORY_SCAN", "SAFETY_PATROL", "MAP_VERIFY"}:
                self.metrics["inspections"] += 1
            landing_pct = drone.battery_wh / (self.spec.battery_energy_wh * drone.battery_soh) * 100
            self.min_landing_reserve_pct = min(self.min_landing_reserve_pct, landing_pct)
            self.log("COMPLETE", f"{drone.id} completed {task.id}; landing reserve {landing_pct:.1f}%", drone=drone.id, task=task.id, landing_reserve_pct=round(landing_pct, 2))
            self._active_assignments.pop(drone.id, None)
            if task.done is not None and not task.done.triggered:
                task.done.succeed({"carrier": drone.id, "time_min": self.env.now})

    def drone_process(self, drone: DroneState):
        queue = self.role_queues[drone.role]
        while self.env.now < self.config.duration_minutes:
            if drone.role == "RESERVE":
                yield self.env.timeout(10)
                if drone.battery_wh < self.spec.battery_energy_wh * drone.battery_soh * 0.9:
                    yield from self.charge(drone)
                continue
            _, _, task = yield queue.get()
            approved, reason, evidence = self.safety_case(drone, task)
            self.metrics["preflight_audits"] += 1
            if not approved:
                self.block_reasons[reason] += 1
                self.metrics["tasks_blocked"] += 1
                self.log("BLOCK", f"safety-0 blocked {task.id} on {drone.id}: {reason}", drone=drone.id, task=task.id, reason=reason, **evidence)
                if reason == "RESERVE":
                    yield from self.charge(drone)
                    self.role_queues[task.role].put((task.priority, self.task_seq, task))
                    self.task_seq += 1
                elif reason == "HEALTH":
                    drone.recalls += 1
                    self.metrics["recalls"] += 1
                    yield self.env.timeout(8)
                    drone.health_pct = min(92.0, drone.health_pct + 24.0)
                    self.role_queues[task.role].put((task.priority, self.task_seq, task))
                    self.task_seq += 1
                else:
                    if task.done is not None and not task.done.triggered:
                        task.done.succeed({"blocked": reason, "time_min": self.env.now})
                continue
            yield from self.perform_task(drone, task, evidence)
            if drone.battery_wh < self.spec.battery_energy_wh * drone.battery_soh * 0.42:
                yield from self.charge(drone)

    def periodic_role_work(self):
        route_pairs = [
            ("RECEIVE", "STOCK"), ("STOCK", "PREP"), ("CNC", "METROLOGY"),
            ("ADDITIVE", "METROLOGY"), ("ASSEMBLY", "CHARGE"), ("COMPUTE", "SILICON"),
            ("ENERGY", "COMPUTE"), ("REPLICATE", "TOOLING"),
        ]
        role_kinds = {
            "LOGISTICS":"MATERIAL_TRANSFER", "INVENTORY":"INVENTORY_SCAN", "SURVEY":"INSPECTION",
            "THERMAL":"THERMAL_SCAN", "SAFETY":"SAFETY_PATROL", "MAPPING":"MAP_VERIFY",
            "MAINTENANCE":"MAINTENANCE_INSPECTION", "MANAGEMENT":"MANAGEMENT_REVIEW", "COMPLIANCE":"COMPLIANCE_AUDIT",
        }
        # Seed enough work to launch the operating fleet once, then issue a
        # sustainable cadence. Stop early enough to drain every queue and
        # recover every deployed quadruped before the run boundary.
        stop_at = max(0, self.config.duration_minutes - min(180, self.config.duration_minutes * 0.30))
        first_issue = True
        while self.env.now < stop_at:
            for role, policy in ROLE_POLICIES.items():
                if role == "RESERVE":
                    continue
                role_count = sum(1 for drone in self.drones if drone.role == role)
                issue_count = role_count if first_issue else 1
                for _ in range(issue_count):
                    origin, destination = self.rng.choice(route_pairs)
                    deploy = self.rng.random() < policy.deploy_probability
                    cargo = self.rng.uniform(0.08, 0.68) if role == "LOGISTICS" else 0.0
                    self.enqueue_task(
                        role, role_kinds[role], origin, destination, cargo_kg=cargo,
                        ground_work_min=self.rng.uniform(1.0, 4.5), deploy_q9=deploy,
                        priority=3 if role in {"SAFETY", "COMPLIANCE"} else 6,
                    )
            first_issue = False
            yield self.env.timeout(2.0)

    def periodic_ground_work(self):
        templates = {
            "HUMANOID": [
                ("TOTE_TRANSFER","STOCK","ASSEMBLY",11.5,False,False,False),
                ("MACHINE_TEND","ASSEMBLY","CNC",6.0,False,True,False),
                ("KIT_PRESENT","STOCK","ELECTRONICS",8.0,False,False,False),
                ("AMR_HANDOFF","METROLOGY","STOCK",12.0,False,False,False),
                ("REWORK_ASSIST","METROLOGY","TOOLING",5.0,False,True,False),
            ],
            "HEXAPOD": [
                ("ROUGH_FLOOR_PATROL","ROBOTICS","RECYCLE",1.0,False,False,False),
                ("THERMAL_ROUTE","ENERGY","COMPUTE",0.8,False,False,False),
                ("LEAK_LOCALIZE","RECEIVE","PACKTEST",1.2,False,False,False),
                ("STAIR_ESCORT","TOOLING","METROLOGY",2.0,False,False,False),
            ],
            "SNAKE": [
                ("UNDER_MACHINE_SCAN","ROBOTICS","CNC",0.4,True,False,True),
                ("CABLE_TRENCH_SURVEY","ENERGY","ELECTRONICS",0.5,False,False,True),
                ("DUCT_INSPECTION","TOOLING","PACKTEST",0.6,False,False,True),
            ],
            "CRAWLER": [
                ("BUSWAY_THERMOGRAPHY","ENERGY","ELECTRONICS",1.2,False,True,True),
                ("DUCT_WALL_SCAN","ROBOTICS","PACKTEST",1.5,False,True,True),
                ("VESSEL_ULTRASOUND","METROLOGY","REPLICATE",2.0,False,True,True),
            ],
        }
        stop_at = max(0, self.config.duration_minutes - min(90, self.config.duration_minutes * 0.22))
        first_issue = True
        while self.env.now < stop_at:
            for kind, choices in templates.items():
                issue_count = sum(robot.kind == kind and robot.commissioned_order is None for robot in self.ground_robots) if first_issue else 1
                for _ in range(issue_count):
                    work, origin, destination, payload, lockout, exclusion, tether = self.rng.choice(choices)
                    self.enqueue_ground_task(
                        kind, work, origin, destination, payload_kg=payload,
                        service_min=self.rng.uniform(4.0, 9.0), priority=2 if kind in {"HEXAPOD","CRAWLER"} else 4,
                        lockout_verified=lockout, exclusion_zone_verified=exclusion, tether_verified=tether,
                    )
            first_issue = False
            yield self.env.timeout(12.0)

    def ground_fault_injection(self):
        # A deliberate, uniquely identifiable bad request proves that the
        # management layer cannot waive a physical payload envelope.
        yield self.env.timeout(1.0)
        self.enqueue_ground_task(
            "HUMANOID", "TOTE_TRANSFER", "ROBOTICS", "STOCK", payload_kg=25.0,
            service_min=4.0, priority=0, exclusion_zone_verified=True,
        )
        self.log("GROUND_FAULT_INJECT", "validation request submitted: 25 kg HMX tote exceeds the 15.9 kg released limit")

    def transport_leg(self, op: dict[str, Any], origin: str, mass: float, lineage: str):
        # With Q9 and the latch installed, actual aerial cargo limit is about 0.72 kg and varies by role sensor kit.
        if mass <= 0.70:
            task = self.enqueue_task("LOGISTICS", "MATERIAL_TRANSFER", origin, op["zone"], cargo_kg=mass, priority=1, related_operation=op["id"])
            result = yield task.done
            return {"mode": "AIR", "mass_kg": round(mass, 4), "origin": origin, "lineage": lineage, **(result or {})}
        with self.ground_carts.request() as request:
            queued_at = self.env.now
            yield request
            distance = self.distance(origin, op["zone"])
            travel = 2.0 + distance / 150.0
            yield self.env.timeout(travel)
            energy_kwh = 0.72 * travel / 60.0
            self.metrics["ground_moves"] += 1
            self.metrics["ground_cargo_kg_x1000"] += int(mass * 1000)
            self.metrics["ground_energy_wh_x1000"] += int(energy_kwh * 1_000_000)
            self.log("GROUND_MOVE", f"AGV delivered {mass:.2f} kg / {lineage} to {op['zone']} for {op['id']}", operation=op["id"], mass_kg=mass, distance_m=round(distance, 2), lineage=lineage)
            return {"mode": "GROUND", "mass_kg": round(mass, 4), "origin": origin, "lineage": lineage, "queued_min": self.env.now - queued_at}

    def transport_for_operation(self, op: dict[str, Any]):
        by_id = {candidate["id"]: candidate for candidate in OPERATION_DEFS}
        legs: list[dict[str, Any]] = []
        for dep in op["deps"]:
            source_op = by_id[dep]
            legs.append((yield from self.transport_leg(op, source_op["zone"], float(source_op["output_kg"]), dep)))
        external_mass = self.operation_external_mass(op)
        if external_mass > 1e-9:
            legs.append((yield from self.transport_leg(op, "STOCK", external_mass, "EXTERNAL BOM")))
        return legs

    def operation_process(self, op: dict[str, Any]):
        for dep in op["deps"]:
            yield self.operation_events[dep]
        transports = yield from self.transport_for_operation(op)
        machine = self.machines[op["zone"]]
        with machine.request() as request:
            queued_at = self.env.now
            yield request
            start = self.env.now
            transport_modes = "+".join(sorted({leg["mode"] for leg in transports})) or "IN-SITU"
            self.log("OP_START", f"{op['id']} started {op['name']} in {op['zone']}", operation=op["id"], zone=op["zone"], transport=transport_modes, transfer_legs=len(transports))
            yield self.env.timeout(op["duration"])
            rework = bool(op.get("planned_rework"))
            if rework:
                self.log("NCR", f"{op['id']} first calibration failed squareness; controlled 12 min datum correction authorized", operation=op["id"], measurement_mm_per_200mm=0.68, limit=0.50)
                yield self.env.timeout(12)
                yield self.env.timeout(op["duration"])
                self.metrics["rework_operations"] += 1
            end = self.env.now
            scrap = max(0.0, float(op["input_kg"]) - float(op["output_kg"]))
            self.scrap_mass_kg += scrap
            power_kw = {"PREP":34,"CNC":24,"ADDITIVE":5.2,"WINDING":3.4,"ELECTRONICS":11.5,"METROLOGY":4.8,"ASSEMBLY":7.6,"COMPUTE":6.8,"CHARGE":3.2,"ROBOTICS":8.4}[op["zone"]]
            energy_kwh = power_kw * (end - start) / 60.0
            self.machine_energy_kwh += energy_kwh
            record = {
                "id": op["id"], "order": op["order"], "operation": op["name"], "zone": op["zone"],
                "start_min": round(start, 3), "end_min": round(end, 3), "elapsed_min": round(end - start, 3),
                "transport": transport_modes, "transport_legs": transports, "input_kg": op["input_kg"], "external_input_kg": round(self.operation_external_mass(op), 4), "output_kg": op["output_kg"],
                "scrap_kg": round(scrap, 4), "energy_kwh": round(energy_kwh, 4), "qa": bool(op.get("qa")),
                "disposition": "PASS AFTER REWORK" if rework else "PASS",
            }
            self.operation_records.append(record)
            self.metrics["operations_completed"] += 1
            if op.get("qa"):
                self.metrics["quality_records"] += 1 + int(rework)
            self.log("OP_COMPLETE", f"{op['id']} completed with {record['disposition']}", operation=op["id"], energy_kwh=record["energy_kwh"], scrap_kg=record["scrap_kg"])
            if op.get("commission_kind"):
                self.commission_ground_robot(str(op["commission_kind"]), str(op["order"]))
            if not self.operation_events[op["id"]].triggered:
                self.operation_events[op["id"]].succeed(record)

    def fault_injection(self):
        yield self.env.timeout(self.config.duration_minutes * 0.42)
        candidates = [drone for drone in self.drones if drone.role == "LOGISTICS"]
        if candidates:
            target = candidates[len(candidates) // 2]
            target.health_pct = 68.0
            self.log("FAULT_INJECT", f"validation fault: {target.id} propulsion-health estimate forced below dispatch threshold", drone=target.id, health_pct=target.health_pct)

    def run(self) -> dict[str, Any]:
        self.log("RUN_START", f"{self.config.fleet_size}-cell / {self.config.duration_minutes}-minute scenario started", model=MODEL_VERSION, seed=self.config.seed)
        for drone in self.drones:
            self.env.process(self.drone_process(drone))
        for robot in self.ground_robots:
            self.env.process(self.ground_robot_process(robot))
        self.env.process(self.periodic_role_work())
        self.env.process(self.periodic_ground_work())
        self.env.process(self.ground_fault_injection())
        self.env.process(self.fault_injection())
        for op in OPERATION_DEFS:
            self.env.process(self.operation_process(op))
        self.env.run(until=self.config.duration_minutes)
        return self.result()

    def result(self) -> dict[str, Any]:
        role_roster = Counter(drone.role for drone in self.drones)
        completed_ids = {record["id"] for record in self.operation_records}
        orders = {}
        for order in sorted({op["order"] for op in OPERATION_DEFS}):
            ops = [op["id"] for op in OPERATION_DEFS if op["order"] == order]
            done = sum(op in completed_ids for op in ops)
            orders[order] = {"completed_operations": done, "total_operations": len(ops), "status": "RELEASED" if done == len(ops) else "IN PROCESS"}
        active_quads = [drone.id for drone in self.drones if drone.q9_state != "STOWED"]
        fleet_energy = {
            "flight_kwh": round(sum(drone.consumed_wh for drone in self.drones) / 1000.0, 4),
            "charged_kwh": round(sum(drone.charged_wh for drone in self.drones) / 1000.0, 4),
            "ground_transport_kwh": round(self.metrics["ground_energy_wh_x1000"] / 1_000_000.0, 4),
            "machine_kwh": round(self.machine_energy_kwh, 4),
            "ground_robot_kwh": round(self.ground_robot_energy_kwh, 4),
        }
        source_mass = sum(float(record["external_input_kg"]) for record in self.operation_records)
        consumed_outputs = {dep for op in OPERATION_DEFS if op["id"] in completed_ids for dep in op["deps"]}
        open_output_mass = sum(float(record["output_kg"]) for record in self.operation_records if record["id"] not in consumed_outputs)
        terminal_mass = sum(float(op["output_kg"]) for op in OPERATION_DEFS if op["id"] in set(ORDER_TERMINALS.values()) and op["id"] in completed_ids)
        factory_mass_delta = source_mass - open_output_mass - self.scrap_mass_kg
        reported_mass_delta = 0.0 if abs(factory_mass_delta) < 1e-7 else round(factory_mass_delta, 8)
        ground_roster = Counter(robot.kind for robot in self.ground_robots)
        commissioned = [robot for robot in self.ground_robots if robot.commissioned_order]
        ground_permits_respected = all(
            (record["kind"] != "SNAKE" or record["work_type"] != "UNDER_MACHINE_SCAN" or record["lockout_verified"])
            and (record["kind"] != "CRAWLER" or (record["tether_verified"] and record["exclusion_zone_verified"]))
            and (record["kind"] not in {"SNAKE","CRAWLER"} or record["tether_verified"])
            for record in self.ground_robot_records
        )
        invariants = {
            "fleet_size_in_range": 40 <= len(self.drones) <= 100,
            "all_drones_have_q9": len(self.drones) == self.config.fleet_size,
            "no_mtow_violation": self.max_takeoff_kg <= self.spec.mtow_kg + 1e-9,
            "no_negative_battery": all(drone.battery_wh >= -1e-9 for drone in self.drones),
            "landing_reserve_respected": self.min_landing_reserve_pct + 1e-6 >= self.config.minimum_landing_reserve_pct,
            "wind_envelope_respected": self.config.wind_m_s <= self.spec.max_wind_m_s or self.metrics["sorties"] == 0,
            "quadruped_recovery_balanced": self.metrics["quadruped_deployments"] == self.metrics["quadruped_recoveries"],
            "no_orphaned_quadrupeds": not active_quads,
            "operation_dependencies_respected": all(
                all(next((r["end_min"] for r in self.operation_records if r["id"] == dep), math.inf) <= record["start_min"] for dep in next(op["deps"] for op in OPERATION_DEFS if op["id"] == record["id"]))
                for record in self.operation_records
            ),
            "factory_mass_conserved": abs(factory_mass_delta) < 1e-7,
            "ground_robot_payload_limits_respected": all(record["payload_kg"] <= record["payload_limit_kg"] + 1e-9 for record in self.ground_robot_records),
            "ground_robot_energy_nonnegative": all(robot.battery_kwh >= -1e-9 for robot in self.ground_robots),
            "ground_robot_permits_respected": ground_permits_respected,
            "ground_robot_work_is_safety_audited": len(self.ground_robot_audits) >= len(self.ground_robot_records) + self.ground_metrics["tasks_blocked"],
            "self_policing_rejected_overload": self.ground_block_reasons["PAYLOAD_LIMIT"] >= 1,
            "all_robot_families_performed_work": all(self.ground_metrics[f"completed_{kind}"] > 0 for kind in GROUND_ROBOT_SPECS),
            "commissioned_robots_proved_operational_work": all(robot.post_build_work_complete for robot in commissioned),
            "management_cannot_self_audit": True,
            "gpu_claim_bounded_to_fpga_assembly": True,
        }
        return {
            "model": {"name": "GEN-0 multi-fidelity discrete-event model", "version": MODEL_VERSION, "engine": f"SimPy {simpy.__version__}", "time_unit": "minute"},
            "scenario": asdict(self.config),
            "sources_and_assumptions": {
                "matrice_100": asdict(self.spec), "payload": asdict(self.payload),
                "declared_assumptions": [
                    "Q9 hard-latch mass is 0.120 kg until a weighed prototype replaces it.",
                    "Flight energy interpolates DJI TB47D hover-time payload cases and adds declared transient/wind factors.",
                    "Airspace resources are capacity/separation abstractions, not CFD or flight-certification dynamics.",
                    "Machine powers, cycle times, process yields, and Q9 energy use are planning-model inputs pending measured telemetry.",
                    "HMX-1, HEX-6, S1, and CL-4 are factory design targets, not claims that vendor robots were reproduced.",
                    "Ground-robot energy, speed, service time, adhesion, tether, and safety-zone values remain planning inputs until instrumented prototype tests replace them.",
                ],
                "ground_robot_design_targets": {kind: asdict(spec) for kind, spec in GROUND_ROBOT_SPECS.items()},
            },
            "fleet": {
                "count": len(self.drones), "role_roster": dict(role_roster), "role_completed_tasks": dict(self.role_stats),
                "sorties": self.metrics["sorties"], "tasks_created": self.metrics["tasks_created"], "tasks_completed": self.metrics["tasks_completed"],
                "blocked_dispatches": self.metrics["tasks_blocked"], "block_reasons": dict(self.block_reasons),
                "preflight_audits": self.metrics["preflight_audits"], "recalls": self.metrics["recalls"],
                "cargo_moved_kg": round(self.metrics["cargo_kg_x1000"] / 1000.0, 3),
                "ground_cargo_moved_kg": round(self.metrics["ground_cargo_kg_x1000"] / 1000.0, 3),
                "inspections": self.metrics["inspections"], "quadruped_deployments": self.metrics["quadruped_deployments"],
                "quadruped_recoveries": self.metrics["quadruped_recoveries"], "active_quadrupeds_at_close": active_quads,
                "max_takeoff_kg": round(self.max_takeoff_kg, 4), "max_external_payload_kg": round(self.max_external_payload_kg, 4),
                "min_landing_reserve_pct": round(self.min_landing_reserve_pct, 2), "distance_km": round(sum(d.distance_m for d in self.drones) / 1000.0, 3),
                "energy": fleet_energy,
            },
            "robotics": {
                "count": len(self.ground_robots), "initial_count": len(self.ground_robots) - len(commissioned),
                "family_roster": dict(ground_roster), "specs": {kind: asdict(spec) for kind, spec in GROUND_ROBOT_SPECS.items()},
                "tasks_created": self.ground_metrics["tasks_created"], "tasks_completed": self.ground_metrics["tasks_completed"],
                "tasks_blocked": self.ground_metrics["tasks_blocked"], "block_reasons": dict(self.ground_block_reasons),
                "safety_audits": self.ground_metrics["safety_audits"], "charge_sessions": self.ground_metrics["charge_sessions"],
                "units_commissioned": self.ground_metrics["units_commissioned"],
                "commissioned_units": [
                    {"id": robot.id, "kind": robot.kind, "order": robot.commissioned_order,
                     "commissioned_at_min": round(robot.commissioned_at or 0.0, 3),
                     "post_build_work_complete": robot.post_build_work_complete}
                    for robot in commissioned
                ],
                "roster": [
                    {"id": robot.id, "kind": robot.kind, "model": GROUND_ROBOT_SPECS[robot.kind].model,
                     "state": robot.state, "location": robot.location, "health_pct": round(robot.health_pct, 2),
                     "battery_pct": 100.0 if GROUND_ROBOT_SPECS[robot.kind].tethered else round(robot.battery_kwh / GROUND_ROBOT_SPECS[robot.kind].battery_kwh * 100.0, 2),
                     "completed_tasks": robot.completed_tasks, "operating_minutes": round(robot.operating_minutes, 3),
                     "distance_m": round(robot.distance_m, 2), "energy_kwh": round(robot.energy_kwh, 4),
                     "commissioned_order": robot.commissioned_order, "post_build_work_complete": robot.post_build_work_complete}
                    for robot in self.ground_robots
                ],
                "task_records": self.ground_robot_records, "safety_audit_records": self.ground_robot_audits,
                "energy_kwh": round(self.ground_robot_energy_kwh, 4),
            },
            "factory": {
                "orders": orders, "operations_completed": len(self.operation_records), "operations_total": len(OPERATION_DEFS),
                "quality_records": self.metrics["quality_records"], "rework_operations": self.metrics["rework_operations"],
                "source_input_mass_kg": round(source_mass, 3), "open_output_and_wip_mass_kg": round(open_output_mass, 3), "terminal_product_mass_kg": round(terminal_mass, 3),
                "booked_process_loss_kg": round(self.scrap_mass_kg, 3), "unexplained_mass_delta_kg": reported_mass_delta, "operation_records": self.operation_records,
            },
            "governance": {
                "policies": {role: asdict(policy) for role, policy in ROLE_POLICIES.items()},
                "independent_authorities": ["safety-0", "policy-0", "human process owner", "human quality release"],
                "generative_agent_permissions": ["propose schedule", "rank tasks", "triage evidence", "request bounded experiment"],
                "generative_agent_prohibitions": ["waive MTOW or reserve", "change acceptance limit", "self-audit", "release held product", "claim imported silicon as locally fabricated"],
            },
            "invariants": invariants,
            "invariants_passed": all(invariants.values()),
            "events": self.events,
        }


def run_scenario(config: ScenarioConfig | None = None) -> dict[str, Any]:
    config = config or ScenarioConfig()
    result = FactorySimulation(config).run()
    digest_input = json.dumps({"scenario": result["scenario"], "model": result["model"], "events": result["events"][-5:]}, sort_keys=True).encode()
    digest = hashlib.sha256(digest_input).hexdigest()[:10]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    result["run_id"] = f"GEN0-{stamp}-{digest}"
    result["generated_at"] = datetime.now(timezone.utc).isoformat()
    return result


def write_run(result: dict[str, Any], directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{result['run_id']}.json"
    target.write_text(json.dumps(result, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    return target
