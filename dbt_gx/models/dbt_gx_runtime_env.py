from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dbt_gx.models.dbt_gx_config import DbtGxConfig
from dbt_gx.models.dbt_profile import DbtProfileConfig


@dataclass
class GbtGxRuntimeEnv:
    project_dir: Path
    dbt_profile_config: DbtProfileConfig = field(default_factory=DbtProfileConfig)
    dbt_gx_config: DbtGxConfig = field(default_factory=DbtGxConfig)
    output: Path = field(default_factory=lambda: Path("test_results.json"))
    run_name: str = "dbt-gx"

    @property
    def site_path(self) -> Path:
        return self.project_dir / "target" / "dbt_gx" / "site"

    @property
    def site_config(self) -> dict[str, Any]:
        return {
            "class_name": "SiteBuilder",
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": str(self.site_path.resolve()),
            },
        }
