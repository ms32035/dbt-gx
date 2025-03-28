"""Configuration model for dbt profiles."""

import os
from argparse import Namespace
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import yaml
from dbt.config.profile import Profile
from dbt.config.renderer import ProfileRenderer
from dbt.flags import set_from_args
from dbt_common.clients.system import get_env
from dbt_common.context import set_invocation_context

DEFAULT_PROFILES_DIR = Path(os.path.expanduser("~/.dbt"))


@dataclass
class DbtProfileConfig:
    """Configuration for dbt profile and target.

    This class handles loading and parsing dbt profiles, including environment
    variable resolution. It uses dbt's built-in profile handling mechanisms
    to ensure compatibility with dbt's profile loading behavior.
    """

    profile_name: str = "default"
    target_name: str | None = None
    profiles_dir: Path | None = None
    target_config: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Set default profiles dir if not provided."""
        if self.profiles_dir is None:
            self.profiles_dir = DEFAULT_PROFILES_DIR

    def _load_profiles(self) -> dict[str, Any]:
        """Load raw profiles from profiles.yml.

        This method is separated to allow easier testing by patching
        the profiles loading mechanism without requiring a file on disk.

        Returns:
            Dictionary containing the raw profiles configuration.

        Raises:
            FileNotFoundError: If profiles.yml does not exist.
            ValueError: If profiles.yml is empty.
        """
        if self.profiles_dir is None:
            raise ValueError("profiles_dir is not set")

        profiles_path = self.profiles_dir / "profiles.yml"
        if not profiles_path.exists():
            raise FileNotFoundError(f"Could not find profiles.yml in {self.profiles_dir}")

        with profiles_path.open() as f:
            raw_profiles = cast(dict[str, Any], yaml.safe_load(f))

        if not raw_profiles:
            raise ValueError("profiles.yml is empty")

        return raw_profiles

    def load_target(self) -> None:
        """Load the target configuration from the dbt profile.

        This method will:
        1. Load the raw profiles from the profiles.yml file
        2. Process any environment variables in the configuration
        3. Extract the specific target configuration based on profile_name and target_name

        Returns:
            Dictionary containing the target configuration.

        Raises:
            FileNotFoundError: If profiles.yml does not exist.
            ValueError: If profiles.yml is empty.
            KeyError: If profile or target does not exist.
            dbt.exceptions.DbtProfileError: If there are issues with the profile configuration.
        """
        raw_profiles = self._load_profiles()

        if self.profile_name not in raw_profiles:
            raise KeyError(f"Profile '{self.profile_name}' not found in profiles.yml")

        # Create renderer with proper context
        set_invocation_context(get_env())
        set_from_args(Namespace(), None)

        profile = Profile.from_raw_profile_info(
            raw_profile=raw_profiles[self.profile_name],
            profile_name=self.profile_name,
            target_override=self.target_name,
            renderer=ProfileRenderer(),
        )

        # Get target config
        target_dict = cast(dict[str, Any], profile.credentials.to_dict())
        target_dict["type"] = profile.to_target_dict()["type"]
        if not target_dict:
            raise KeyError(f"Target '{self.target_name or 'default'}' not found in profile '{self.profile_name}'")

        self.target_config = target_dict
