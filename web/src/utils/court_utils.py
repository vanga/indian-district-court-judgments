"""
Court Utility Functions for District Court Judgments
Handles state/district/complex mappings and court hierarchy
"""

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class CourtComplex:
    """Represents a court complex"""

    state_code: str
    state_name: str
    district_code: str
    district_name: str
    complex_code: str
    complex_name: str
    court_numbers: str  # Comma-separated court numbers e.g., "10,11,12"
    flag: str  # Usually "N"

    @property
    def complex_code_full(self) -> str:
        """Get the full complex code in format: {complex_id}@{court_numbers}@{flag}"""
        return f"{self.complex_code}@{self.court_numbers}@{self.flag}"


def load_courts_csv(csv_path: Path) -> List[CourtComplex]:
    """
    Load court hierarchy from CSV file

    Args:
        csv_path: Path to the courts.csv file

    Returns:
        List of CourtComplex objects
    """
    courts = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            courts.append(
                CourtComplex(
                    state_code=row["state_code"],
                    state_name=row["state_name"],
                    district_code=row["district_code"],
                    district_name=row["district_name"],
                    complex_code=row["complex_code"],
                    complex_name=row["complex_name"],
                    court_numbers=row["court_numbers"],
                    flag=row["flag"],
                )
            )
    return courts


def save_courts_csv(courts: List[CourtComplex], csv_path: Path):
    """
    Save court hierarchy to CSV file

    Args:
        courts: List of CourtComplex objects
        csv_path: Path to save the CSV file
    """
    fieldnames = [
        "state_code",
        "state_name",
        "district_code",
        "district_name",
        "complex_code",
        "complex_name",
        "court_numbers",
        "flag",
    ]

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for court in courts:
            writer.writerow(
                {
                    "state_code": court.state_code,
                    "state_name": court.state_name,
                    "district_code": court.district_code,
                    "district_name": court.district_name,
                    "complex_code": court.complex_code,
                    "complex_name": court.complex_name,
                    "court_numbers": court.court_numbers,
                    "flag": court.flag,
                }
            )


def filter_courts_by_state(
    courts: List[CourtComplex], state_code: str
) -> List[CourtComplex]:
    """Filter courts by state code"""
    return [c for c in courts if c.state_code == state_code]


def filter_courts_by_district(
    courts: List[CourtComplex], state_code: str, district_code: str
) -> List[CourtComplex]:
    """Filter courts by state and district code"""
    return [
        c
        for c in courts
        if c.state_code == state_code and c.district_code == district_code
    ]


def get_court_by_complex(
    courts: List[CourtComplex], state_code: str, district_code: str, complex_code: str
) -> Optional[CourtComplex]:
    """Get a specific court complex"""
    for court in courts:
        if (
            court.state_code == state_code
            and court.district_code == district_code
            and court.complex_code == complex_code
        ):
            return court
    return None


def get_unique_states(courts: List[CourtComplex]) -> List[tuple]:
    """Get unique states from courts list as (state_code, state_name) tuples"""
    seen = set()
    result = []
    for court in courts:
        key = court.state_code
        if key not in seen:
            seen.add(key)
            result.append((court.state_code, court.state_name))
    return result


def get_unique_districts(courts: List[CourtComplex], state_code: str) -> List[tuple]:
    """Get unique districts for a state as (district_code, district_name) tuples"""
    seen = set()
    result = []
    for court in courts:
        if court.state_code != state_code:
            continue
        key = court.district_code
        if key not in seen:
            seen.add(key)
            result.append((court.district_code, court.district_name))
    return result
