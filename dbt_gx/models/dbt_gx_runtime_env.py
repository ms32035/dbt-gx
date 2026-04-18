from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dbt_gx.models.dbt_gx_config import DbtGxConfig
from dbt_gx.models.dbt_profile import DbtProfileConfig


@dataclass
class DbtGxRuntimeEnv:
    """Runtime configuration for a dbt-gx execution.

    Holds the project directory, dbt profile, dbt-gx config, and run metadata.
    """

    project_dir: Path
    dbt_profile_config: DbtProfileConfig = field(default_factory=DbtProfileConfig)
    dbt_gx_config: DbtGxConfig = field(default_factory=DbtGxConfig)
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
