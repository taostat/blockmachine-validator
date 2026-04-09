"""
Loads the method verification registry from methods.json.
Same query interface as before, data now lives in a shared JSON file
that the Rust gateway can also consume.
"""

import json
import logging
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class BlockParamType(Enum):
    HASH = "hash"
    NUMBER = "number"
    HEX_NUMBER = "hex_number"


_BLOCK_PARAM_TYPE_MAP = {
    "hash": BlockParamType.HASH,
    "number": BlockParamType.NUMBER,
    "hex_number": BlockParamType.HEX_NUMBER,
}


class MethodRegistry:
    def __init__(self):
        # (chain, method) -> spec dict from json
        self._specs: dict[tuple[str, str], dict] = {}
        self._chain_aliases: dict[str, str] = {}

    def is_verifiable(self, chain: str, method: str) -> bool:
        spec = self._lookup(chain, method)
        if spec is None:
            return False
        return spec.get("verifiable", False)

    def requires_block_param(self, chain: str, method: str) -> tuple[bool, int]:
        """returns (needs_block, block_param_index)"""
        spec = self._lookup(chain, method)
        if spec is None:
            return (False, -1)
        idx = spec.get("block_param_index", -1)
        if idx < 0:
            return (False, -1)
        return (True, idx)

    def get_block_param_type(self, chain: str, method: str) -> Optional[BlockParamType]:
        spec = self._lookup(chain, method)
        if spec is None:
            return None
        raw = spec.get("block_param_type")
        if raw is None:
            return None
        return _BLOCK_PARAM_TYPE_MAP.get(raw)

    def _lookup(self, chain: str, method: str) -> Optional[dict]:
        normalized = self._chain_aliases.get(chain.upper(), chain.upper())
        return self._specs.get((normalized, method))

    def normalize_chain(self, chain: str) -> str:
        return self._chain_aliases.get(chain.upper(), chain.upper())


def load_registry(path: Optional[str] = None) -> MethodRegistry:
    if path is None:
        path = str(Path(__file__).parent.parent / "data" / "methods.json")

    with open(path) as f:
        data = json.load(f)

    reg = MethodRegistry()

    reg._chain_aliases = {
        k.upper(): v.upper() for k, v in data.get("chain_aliases", {}).items()
    }

    methods = data.get("methods", {})
    inherit = data.get("inherit", {})

    for chain, chain_methods in methods.items():
        chain_upper = chain.upper()
        for method_name, spec in chain_methods.items():
            reg._specs[(chain_upper, method_name)] = spec

    for child, parent in inherit.items():
        child_upper = child.upper()
        parent_upper = parent.upper()
        parent_methods = methods.get(parent, methods.get(parent_upper, {}))
        for method_name, spec in parent_methods.items():
            if (child_upper, method_name) not in reg._specs:
                reg._specs[(child_upper, method_name)] = spec

    logger.info(f"Loaded method registry: {len(reg._specs)} method specs")
    return reg


# module-level singleton
_registry: Optional[MethodRegistry] = None


def get_registry() -> MethodRegistry:
    global _registry
    if _registry is None:
        _registry = load_registry()
    return _registry


# convenience functions matching the old interface


def is_verifiable(chain: str, method: str) -> bool:
    return get_registry().is_verifiable(chain, method)


def requires_block_param(chain: str, method: str) -> tuple[bool, Optional[str], int]:
    """returns (needs_block, param_name, param_index) - param_name kept for compat"""
    reg = get_registry()
    needs, idx = reg.requires_block_param(chain, method)
    return (needs, None, idx)


def get_block_param_type(chain: str, method: str) -> Optional[BlockParamType]:
    return get_registry().get_block_param_type(chain, method)
