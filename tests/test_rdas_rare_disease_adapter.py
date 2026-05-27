from src.constants import DataSourceName
from src.input_adapters.rdas.diseases import RDASRareDiseaseAdapter
from src.models.disease import Disease


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def test_rdas_adapter_emits_rare_gard_diseases(monkeypatch):
    calls = []

    def fake_post(url, json, timeout):
        calls.append((url, json, timeout))
        offset = json["variables"]["offset"]
        if offset == 0:
            return _FakeResponse({
                "data": {
                    "gards": [
                        {"GardId": "GARD:1", "GardName": "GRACILE syndrome"},
                        {"GardId": "GARD:0000005", "GardName": "Achondrogenesis"},
                    ]
                }
            })
        return _FakeResponse({"data": {"gards": []}})

    monkeypatch.setattr("src.input_adapters.rdas.diseases.requests.post", fake_post)

    adapter = RDASRareDiseaseAdapter(
        graphql_url="https://example.org/graphql",
        batch_size=2,
        download_date="2026-05-27",
    )
    batches = list(adapter.get_all())
    diseases = [node for batch in batches for node in batch]

    assert adapter.get_datasource_name() == DataSourceName.RDAS
    assert adapter.get_version().download_date.isoformat() == "2026-05-27"
    assert all(isinstance(disease, Disease) for disease in diseases)
    assert [(disease.id, disease.name, disease.rare_disease) for disease in diseases] == [
        ("GARD:0000001", "GRACILE syndrome", True),
        ("GARD:0000005", "Achondrogenesis", True),
    ]
    assert calls[0][1]["variables"] == {"limit": 2, "offset": 0}


def test_rdas_adapter_dedupes_and_skips_invalid_gard_ids(monkeypatch):
    def fake_post(url, json, timeout):
        return _FakeResponse({
            "data": {
                "gards": [
                    {"GardId": "GARD:1", "GardName": "First"},
                    {"GardId": "gard:0000001", "GardName": "Duplicate"},
                    {"GardId": "OMIM:1", "GardName": "Wrong prefix"},
                    {"GardId": "", "GardName": "Blank"},
                ]
            }
        })

    monkeypatch.setattr("src.input_adapters.rdas.diseases.requests.post", fake_post)

    adapter = RDASRareDiseaseAdapter(batch_size=10)
    diseases = [node for batch in adapter.get_all() for node in batch]

    assert len(diseases) == 1
    assert diseases[0].id == "GARD:0000001"
    assert diseases[0].rare_disease is True
