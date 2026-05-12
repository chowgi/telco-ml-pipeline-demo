import random
from dataclasses import dataclass, field
from typing import List


@dataclass
class CellTower:
    cell_id: str
    region: str
    latitude: float
    longitude: float
    technology: str
    capacity: int

    def generate_imsi(self) -> str:
        return f"505{random.randint(10, 99)}{random.randint(10000000, 99999999)}"


def create_cell_towers(regions: List[dict], num_towers: int = 50) -> List[CellTower]:
    towers = []
    for i in range(num_towers):
        region = random.choice(regions)
        towers.append(CellTower(
            cell_id=f"CELL_{i:04d}",
            region=region["name"],
            latitude=region["lat"] + random.uniform(-0.1, 0.1),
            longitude=region["lng"] + random.uniform(-0.1, 0.1),
            technology=random.choice(["4G", "5G"]),
            capacity=random.randint(100, 1000),
        ))
    return towers
