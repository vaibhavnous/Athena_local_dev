from __future__ import annotations

from types import SimpleNamespace

from langchain_core.messages import HumanMessage, SystemMessage

from utilis.llm_usage import (
    LLMUsage,
    combine_llm_usage,
    metered_embedding_usage,
    metered_invoke,
)


class _ProviderUsageLLM:
    model_name = "gpt-4o-mini"

    def invoke(self, prompt, config=None, **kwargs):
        for callback in (config or {}).get("callbacks", []):
            callback.on_llm_end(
                SimpleNamespace(
                    llm_output={
                        "token_usage": {
                            "prompt_tokens": 11,
                            "completion_tokens": 7,
                            "total_tokens": 18,
                        }
                    }
                )
            )
        return SimpleNamespace(content="provider counted")


class _NoUsageLLM:
    model_name = "gpt-4o-mini"

    def invoke(self, prompt, config=None, **kwargs):
        return SimpleNamespace(content="fallback counted")


def test_metered_invoke_prefers_provider_usage():
    result = metered_invoke(
        _ProviderUsageLLM(),
        [SystemMessage(content="system"), HumanMessage(content="hello")],
        provider="azure_openai",
    )

    assert result.usage.input_tokens == 11
    assert result.usage.output_tokens == 7
    assert result.usage.token_count == 18
    assert result.usage.usage_source == "provider"
    assert result.usage.cost_usd > 0


def test_metered_invoke_uses_tiktoken_fallback_without_provider_usage():
    result = metered_invoke(
        _NoUsageLLM(),
        [HumanMessage(content="estimate this prompt")],
        provider="azure_openai",
    )

    assert result.usage.input_tokens > 0
    assert result.usage.output_tokens > 0
    assert result.usage.usage_source == "tiktoken_estimate"
    assert result.usage.cost_usd > 0


def test_combine_llm_usage_sums_tokens_cost_and_calls():
    combined = combine_llm_usage(
        [
            LLMUsage(input_tokens=10, output_tokens=2, cost_usd=0.1, provider="azure_openai", model_name="gpt-4o-mini", usage_source="provider", call_count=1),
            LLMUsage(input_tokens=5, output_tokens=3, cost_usd=0.2, provider="azure_openai", model_name="gpt-4o-mini", usage_source="provider", call_count=1),
        ]
    )

    assert combined.input_tokens == 15
    assert combined.output_tokens == 5
    assert combined.token_count == 20
    assert combined.cost_usd == 0.3
    assert combined.call_count == 2
    assert combined.usage_source == "provider"


def test_metered_embedding_usage_reads_provider_usage():
    response = SimpleNamespace(usage=SimpleNamespace(prompt_tokens=13, total_tokens=13))
    usage = metered_embedding_usage(["claim severity"], response, provider="openai", model_name="text-embedding-3-small")

    assert usage.input_tokens == 13
    assert usage.output_tokens == 0
    assert usage.token_count == 13
    assert usage.usage_source == "provider"
