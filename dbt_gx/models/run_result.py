from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class RunResult:
    """Class to represent the result of a dbt run."""

    run: dict[str, Any]
    results: list[dict[str, Any]]
    end_time: datetime
    provider: str = "great_expectations"
    version: int = 1

    def run_stats(self) -> dict[str, int]:
        """Return counts of suites and expectations, split by pass/fail."""
        suites = 0
        suites_success = 0

        expectations = 0
        expectations_success = 0
        for result in self.results:
            if result.get("success"):
                suites_success += 1
            suites += 1

            results = result.get("results", [])
            expectations += len(results)
            expectations_success += sum(1 for exp in results if exp.get("success"))
        return {
            "suites_total": suites,
            "suites_success": suites_success,
            "expectations_total": expectations,
            "expectations_success": expectations_success,
        }

    @staticmethod
    def serializer(obj: Any) -> str:
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
