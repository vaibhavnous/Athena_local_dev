from utilis.embeddings import _OpenAIEmbeddingAdapter


class _Embeddings:
    def __init__(self):
        self.request = None

    def create(self, **request):
        self.request = request
        return type("Response", (), {"data": [type("Item", (), {"index": 0, "embedding": [1.0]})()]})()


def test_openai_adapter_requests_configured_dimensions():
    embeddings = _Embeddings()
    client = type("Client", (), {"embeddings": embeddings})()
    adapter = _OpenAIEmbeddingAdapter(client=client, model_name="text-embedding-3-small", dimensions=1024)

    assert adapter.embed_query("claims") == [1.0]
    assert embeddings.request["dimensions"] == 1024
