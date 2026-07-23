from __future__ import annotations

import unittest

from simulation.model import FactorySimulation, Matrice100Spec, ScenarioConfig, run_scenario


class FactorySimulationTests(unittest.TestCase):
    def test_published_hover_interpolation(self) -> None:
        spec = Matrice100Spec()
        self.assertEqual(spec.hover_minutes(0), 22)
        self.assertEqual(spec.hover_minutes(0.5), 17)
        self.assertEqual(spec.hover_minutes(1.0), 13)
        self.assertAlmostEqual(spec.hover_minutes(0.25), 19.5)

    def test_dynamic_cargo_limit_includes_q9_and_latch(self) -> None:
        sim = FactorySimulation(ScenarioConfig(fleet_size=40, duration_minutes=60))
        logistics = next(drone for drone in sim.drones if drone.role == "LOGISTICS")
        self.assertAlmostEqual(sim.cargo_limit(logistics), 0.722, places=3)

    def test_full_100_cell_shift_obeys_invariants(self) -> None:
        result = run_scenario(ScenarioConfig(fleet_size=100, duration_minutes=720, seed=903742))
        self.assertTrue(result["invariants_passed"], result["invariants"])
        self.assertEqual(result["fleet"]["count"], 100)
        self.assertGreater(result["fleet"]["sorties"], 100)
        self.assertGreater(result["fleet"]["quadruped_deployments"], 0)
        self.assertEqual(result["fleet"]["quadruped_deployments"], result["fleet"]["quadruped_recoveries"])
        self.assertEqual(result["factory"]["operations_completed"], 44)
        self.assertEqual(len(result["factory"]["orders"]), 7)
        self.assertEqual(result["factory"]["orders"]["WO-G1-004"]["status"], "RELEASED")
        self.assertEqual(result["factory"]["orders"]["WO-HM-001"]["status"], "RELEASED")
        self.assertLessEqual(result["fleet"]["max_takeoff_kg"], 3.6)
        self.assertTrue(result["invariants"]["factory_mass_conserved"])
        self.assertEqual(result["factory"]["unexplained_mass_delta_kg"], 0.0)
        transport_modes = {
            leg["mode"]
            for operation in result["factory"]["operation_records"]
            for leg in operation["transport_legs"]
        }
        self.assertEqual(transport_modes, {"AIR", "GROUND"})
        self.assertEqual(result["robotics"]["count"], 15)
        self.assertEqual(result["robotics"]["units_commissioned"], 4)
        self.assertEqual(result["robotics"]["block_reasons"]["PAYLOAD_LIMIT"], 1)
        self.assertTrue(all(unit["post_build_work_complete"] for unit in result["robotics"]["commissioned_units"]))
        self.assertEqual({record["kind"] for record in result["robotics"]["task_records"]}, {"HUMANOID", "HEXAPOD", "SNAKE", "CRAWLER"})

    def test_partial_shift_keeps_open_wip_in_mass_ledger(self) -> None:
        result = run_scenario(ScenarioConfig(fleet_size=100, duration_minutes=120, seed=903742))
        self.assertTrue(result["invariants"]["factory_mass_conserved"])
        self.assertEqual(result["factory"]["unexplained_mass_delta_kg"], 0.0)
        self.assertGreater(result["factory"]["open_output_and_wip_mass_kg"], 0.0)
        self.assertLess(result["factory"]["operations_completed"], result["factory"]["operations_total"])

    def test_overweight_task_is_blocked(self) -> None:
        sim = FactorySimulation(ScenarioConfig(fleet_size=40, duration_minutes=60))
        logistics = next(drone for drone in sim.drones if drone.role == "LOGISTICS")
        task = sim.enqueue_task("LOGISTICS", "MATERIAL_TRANSFER", "STOCK", "CNC", cargo_kg=0.8)
        approved, reason, evidence = sim.safety_case(logistics, task)
        self.assertFalse(approved)
        self.assertIn(reason, {"MTOW", "CARGO_LIMIT"})
        self.assertGreater(evidence["takeoff_kg"], 3.6)


if __name__ == "__main__":
    unittest.main()
