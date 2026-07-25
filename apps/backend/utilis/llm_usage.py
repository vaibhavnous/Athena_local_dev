from __future__ import annotations

import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from langchain_core.callbacks.base import BaseCallbackHandler


_LLM_PRICING: Dict[str, Dict[str, float]] = {
    "gpt-4o": {"input": 0.005, "output": 0.015},
    "gpt-4o-mini": {"input": 0.00015, "output": 0.0006},
    "gpt-4-turbo": {"input": 0.01, "output": 0.03},
    "gpt-35-turbo": {"input": 0.0005, "output": 0.0015},
    "claude-3-5-sonnet": {"input": 0.003, "output": 0.015},
    "claude-3-haiku": {"input": 0.00025, "output": 0.00125},
    "text-embedding-3-small": {"input": 0.00002, "output": 0.0},
    "text-embedding-3-large": {"input": 0.00013, "output": 0.0},
    "_default": {"input": 0.001, "output": 0.002},
}

_ACTIVE_MODEL = (
    os.getenv("AZURE_OPENAI_DEPLOYMENT")
    or os.getenv("AZURE_OPENAI_MODEL")
    or os.getenv("OPENAI_MODEL")
    or "gpt-4o-mini"
)


@dataclass(frozen=True)
class LLMUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    provider: str = ""
    model_name: str = ""
    usage_source: str = "none"
    call_count: int = 0

    @property
    def token_count(self) -> int:
        return int(self.input_tokens or 0) + int(self.output_tokens or 0)

    @property
    def total_tokens(self) -> int:
        return self.token_count

    def to_payload(self) -> Dict[str, Any]:
        return {
            "input_tokens": int(self.input_tokens or 0),
            "output_tokens": int(self.output_tokens or 0),
            "token_count": self.token_count,
            "cost_usd": round(float(self.cost_usd or 0.0), 6),
            "provider": self.provider or None,
            "model": self.model_name or None,
            "usage_source": self.usage_source,
            "call_count": int(self.call_count or 0),
        }

    def ai_store_kwargs(self) -> Dict[str, int]:
        return {
            "token_count": self.token_count,
            "input_tokens": int(self.input_tokens or 0),
            "output_tokens": int(self.output_tokens or 0),
        }

    @classmethod
    def from_payload(cls, payload: Any) -> "LLMUsage":
        if not isinstance(payload, dict):
            return cls()
        return cls(
            input_tokens=int(payload.get("input_tokens") or 0),
            output_tokens=int(payload.get("output_tokens") or 0),
            cost_usd=float(payload.get("cost_usd") or 0.0),
            provider=str(payload.get("provider") or ""),
            model_name=str(payload.get("model") or payload.get("model_name") or ""),
            usage_source=str(payload.get("usage_source") or "none"),
            call_count=int(payload.get("call_count") or (1 if payload.get("token_count") else 0)),
        )


@dataclass(frozen=True)
class MeteredLLMResponse:
    response: Any
    usage: LLMUsage


class MeteredUsageError(Exception):
    def __init__(self, message: str, usage: LLMUsage) -> None:
        super().__init__(message)
        self.usage = usage


class TokenAccumulator(BaseCallbackHandler):
    def __init__(self) -> None:
        super().__init__()
        self.total_input: int = 0
        self.total_output: int = 0
        self.seen_provider_usage: bool = False

    @property
    def total(self) -> int:
        return self.total_input + self.total_output

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        usage = extract_provider_usage(response)
        if usage:
            input_tokens, output_tokens = usage
            self.total_input += input_tokens
            self.total_output += output_tokens
            self.seen_provider_usage = True

    def reset(self) -> None:
        self.total_input = 0
        self.total_output = 0
        self.seen_provider_usage = False


def active_model_name() -> str:
    return _ACTIVE_MODEL


def set_active_model(model_name: str) -> None:
    global _ACTIVE_MODEL
    if model_name:
        _ACTIVE_MODEL = model_name


def compute_cost_usd(input_tokens: int, output_tokens: int, model_name: Optional[str] = None) -> float:
    model = str(model_name or _ACTIVE_MODEL or "").lower()
    pricing = _pricing_for_model(model)
    cost = (int(input_tokens or 0) / 1000) * pricing["input"]
    cost += (int(output_tokens or 0) / 1000) * pricing["output"]
    return round(cost, 6)


def combine_llm_usage(usages: Iterable[LLMUsage]) -> LLMUsage:
    materialized = [usage for usage in usages if isinstance(usage, LLMUsage)]
    if not materialized:
        return LLMUsage()
    input_tokens = sum(int(usage.input_tokens or 0) for usage in materialized)
    output_tokens = sum(int(usage.output_tokens or 0) for usage in materialized)
    cost_usd = round(sum(float(usage.cost_usd or 0.0) for usage in materialized), 6)
    call_count = sum(int(usage.call_count or 0) for usage in materialized)
    providers = {usage.provider for usage in materialized if usage.provider}
    models = {usage.model_name for usage in materialized if usage.model_name}
    sources = {usage.usage_source for usage in materialized if usage.usage_source and usage.usage_source != "none"}
    return LLMUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_usd=cost_usd,
        provider=providers.pop() if len(providers) == 1 else "mixed" if providers else "",
        model_name=models.pop() if len(models) == 1 else "mixed" if models else "",
        usage_source=sources.pop() if len(sources) == 1 else "mixed" if sources else "none",
        call_count=call_count,
    )


def metered_invoke(
    llm: Any,
    prompt: Any,
    *,
    provider: str = "",
    model_name: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> MeteredLLMResponse:
    resolved_model = model_name or _model_name_from_llm(llm) or _ACTIVE_MODEL
    estimated_input = estimate_tokens(prompt, resolved_model)
    token_acc = TokenAccumulator()
    merged_config = _with_callback(config, token_acc)

    try:
        response = llm.invoke(prompt, config=merged_config, **kwargs)
    except TypeError as exc:
        if "config" not in str(exc):
            raise
        response = llm.invoke(prompt, **kwargs)

    provider_usage = (token_acc.total_input, token_acc.total_output) if token_acc.total else extract_provider_usage(response)
    if provider_usage:
        input_tokens, output_tokens = provider_usage
        usage_source = "provider"
    else:
        input_tokens = estimated_input
        output_tokens = estimate_tokens(_response_content(response), resolved_model)
        usage_source = "tiktoken_estimate"

    usage = LLMUsage(
        input_tokens=int(input_tokens or 0),
        output_tokens=int(output_tokens or 0),
        cost_usd=compute_cost_usd(input_tokens, output_tokens, resolved_model),
        provider=provider,
        model_name=resolved_model or "",
        usage_source=usage_source,
        call_count=1,
    )
    return MeteredLLMResponse(response=response, usage=usage)


def metered_embedding_usage(
    texts: Sequence[str],
    response: Any,
    *,
    provider: str = "",
    model_name: Optional[str] = None,
) -> LLMUsage:
    resolved_model = model_name or _ACTIVE_MODEL
    usage = _usage_from_mapping(getattr(response, "usage", None))
    if usage:
        input_tokens, _ = usage
        usage_source = "provider"
    else:
        input_tokens = estimate_tokens(list(texts), resolved_model)
        usage_source = "tiktoken_estimate"
    return LLMUsage(
        input_tokens=int(input_tokens or 0),
        output_tokens=0,
        cost_usd=compute_cost_usd(input_tokens, 0, resolved_model),
        provider=provider,
        model_name=resolved_model or "",
        usage_source=usage_source,
        call_count=1,
    )


def estimate_tokens(value: Any, model_name: Optional[str] = None) -> int:
    text = _prompt_to_text(value)
    if not text:
        return 0
    try:
        import tiktoken

        try:
            encoding = tiktoken.encoding_for_model(str(model_name or _ACTIVE_MODEL))
        except Exception:
            encoding = tiktoken.get_encoding("cl100k_base")
        return len(encoding.encode(text))
    except Exception:
        return max(1, len(text) // 4)


def extract_provider_usage(response: Any) -> Optional[tuple[int, int]]:
    candidates: List[Any] = [
        getattr(response, "usage_metadata", None),
        getattr(response, "response_metadata", None),
        getattr(response, "llm_output", None),
        getattr(getattr(response, "message", None), "usage_metadata", None),
        getattr(getattr(response, "message", None), "response_metadata", None),
    ]
    for candidate in list(candidates):
        if isinstance(candidate, dict):
            candidates.extend(
                [
                    candidate.get("token_usage"),
                    candidate.get("usage"),
                    candidate.get("usage_metadata"),
                ]
            )

    generations = getattr(response, "generations", None)
    if generations:
        for generation_group in generations:
            for generation in generation_group if isinstance(generation_group, list) else [generation_group]:
                candidates.extend(
                    [
                        getattr(generation, "usage_metadata", None),
                        getattr(generation, "response_metadata", None),
                        getattr(getattr(generation, "message", None), "usage_metadata", None),
                        getattr(getattr(generation, "message", None), "response_metadata", None),
                    ]
                )

    for candidate in candidates:
        usage = _usage_from_mapping(candidate)
        if usage:
            return usage
    return None


def _pricing_for_model(model_name: str) -> Dict[str, float]:
    override = _pricing_override().get(model_name)
    if override:
        return override
    if model_name in _LLM_PRICING:
        return _LLM_PRICING[model_name]
    for known_model, pricing in _LLM_PRICING.items():
        if known_model != "_default" and known_model in model_name:
            return pricing
    return _LLM_PRICING["_default"]


def _pricing_override() -> Dict[str, Dict[str, float]]:
    raw = os.getenv("ATHENA_LLM_PRICING_JSON", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    overrides: Dict[str, Dict[str, float]] = {}
    if isinstance(parsed, dict):
        for model, value in parsed.items():
            if isinstance(value, dict):
                overrides[str(model).lower()] = {
                    "input": float(value.get("input", value.get("input_per_1k", 0.0)) or 0.0),
                    "output": float(value.get("output", value.get("output_per_1k", 0.0)) or 0.0),
                }
    return overrides


def _usage_from_mapping(value: Any) -> Optional[tuple[int, int]]:
    if value is None:
        return None
    if not isinstance(value, dict):
        value = {
            "prompt_tokens": getattr(value, "prompt_tokens", None),
            "completion_tokens": getattr(value, "completion_tokens", None),
            "total_tokens": getattr(value, "total_tokens", None),
        }
    input_tokens = _first_int(value, "prompt_tokens", "input_tokens")
    output_tokens = _first_int(value, "completion_tokens", "output_tokens")
    total_tokens = _first_int(value, "total_tokens")
    if input_tokens is None and output_tokens is None and total_tokens is None:
        return None
    input_tokens = int(input_tokens or 0)
    output_tokens = int(output_tokens or 0)
    if total_tokens is not None and output_tokens == 0:
        output_tokens = max(0, int(total_tokens) - input_tokens)
    return input_tokens, output_tokens


def _first_int(value: Dict[str, Any], *keys: str) -> Optional[int]:
    for key in keys:
        raw = value.get(key)
        if raw is not None:
            try:
                return int(raw)
            except (TypeError, ValueError):
                continue
    return None


def _with_callback(config: Optional[Dict[str, Any]], callback: BaseCallbackHandler) -> Dict[str, Any]:
    merged = dict(config or {})
    callbacks = list(merged.get("callbacks") or [])
    callbacks.append(callback)
    merged["callbacks"] = callbacks
    return merged


def _model_name_from_llm(llm: Any) -> str:
    for attr in ("model_name", "model", "deployment_name", "azure_deployment", "deployment"):
        value = getattr(llm, attr, None)
        if value:
            return str(value)
    return ""


def _response_content(response: Any) -> Any:
    return getattr(response, "content", response)


def _prompt_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, default=str)
    if isinstance(value, Iterable):
        parts: List[str] = []
        for item in value:
            content = getattr(item, "content", item)
            parts.append(_prompt_to_text(content))
        return "\n".join(part for part in parts if part)
    return str(value)
