"""Temporary TEST ONLY contract package for the standalone native sandbox tests."""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

from tools import verify_contract_release as contracts


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def write_complete_synthetic_contract(root: Path, *, schema_overrides: dict[str, dict] | None = None,
                                      adapter_source: bytes | None = None) -> tuple[Path, Path, str]:
    """Write a multi-schema, multi-fixture release plus explicitly synthetic evidence."""
    package = root / "package"
    evidence_root = root / "synthetic-evidence"
    package.mkdir(parents=True, exist_ok=True)
    evidence_root.mkdir(parents=True, exist_ok=True)
    evidence_rows: list[dict] = []
    evidence_serial = 0

    def add_evidence(owner: str, reference: str, subjects: dict[str, str]) -> dict:
        nonlocal evidence_serial
        evidence_serial += 1
        evidence_id = f"TEST-ONLY-{evidence_serial:04d}"
        revision = "a" * 40
        identity = {
            "format": contracts.OWNER_EVIDENCE_FORMAT,
            "evidence_id": evidence_id,
            "owner": owner,
            "repository": owner,
            "revision": revision,
            "decision_ref": reference,
            "status": "accepted",
            "subjects": subjects,
        }
        artifact = _json_bytes(identity)
        artifact_path = evidence_root / (evidence_id + ".json")
        artifact_path.write_bytes(artifact)
        evidence_rows.append({
            **{key: identity[key] for key in ("evidence_id", "owner", "repository", "revision",
                                               "decision_ref", "status", "subjects")},
            "evidence_path": artifact_path.name,
            "evidence_sha256": hashlib.sha256(artifact).hexdigest(),
        })
        return identity

    def make_approval(owner: str, role: str, reference: str, subject_id: str,
                      subject_sha256: str) -> dict:
        record = add_evidence(owner, reference, {subject_id: subject_sha256})
        return {
            "status": "accepted",
            "owner_role": role,
            "owner_repository": owner,
            "revision": record["revision"],
            "decision_ref": reference,
            "subject_id": subject_id,
            "subject_sha256": subject_sha256,
            "evidence_id": record["evidence_id"],
        }

    def artifact(relative: str, raw: bytes, **fields: object) -> dict:
        target = package / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        return {"path": relative, "sha256": hashlib.sha256(raw).hexdigest(), **fields}

    outcome_values = sorted(set(contracts.REQUIRED_CASES.values()))
    schemas: list[dict] = []
    inventory_schemas: list[dict] = []
    field_crosswalk: list[dict] = []
    schema_by_primitive: dict[str, dict] = {}
    for primitive in ("none", "choice", "score", "noul"):
        schema_id = "SYNTH-" + primitive
        contract_id = "SYNTH-CONTRACT-" + primitive
        schema = (schema_overrides or {}).get(primitive, {
            "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
            "type": "object",
            "required": ["decision", "value"],
            "properties": {
                "decision": {"type": "string", "enum": outcome_values},
                "value": {"type": "integer"},
            },
            "additionalProperties": False,
        })
        schema_raw = _json_bytes(schema)
        schema_artifact = artifact("schemas/" + primitive + ".json", schema_raw,
                                   schema_id=schema_id, media_type="application/json")
        schemas.append(schema_artifact)
        schema_by_primitive[primitive] = schema_artifact
        inventory_schemas.append({
            "schema_id": schema_id,
            "schema_sha256": schema_artifact["sha256"],
            "contract_id": contract_id,
            "primitive": primitive,
            "field_paths": ["/decision", "/value"],
            "aliases": [],
        })
        for field_path in ("/decision", "/value"):
            row = {
                "schema_id": schema_id,
                "schema_sha256": schema_artifact["sha256"],
                "contract_id": contract_id,
                "primitive": primitive,
                "field_path": field_path,
                "meaning": "TEST ONLY synthetic schema property",
                "producer_role": "Agent and research",
                "producer_leaf": "https://github.com/Jevelopers-neatHack-2026/agent-harness/issues/1",
                "consumer_role": "Creative and fulfillment",
                "consumer_leaf": "https://github.com/Jevelopers-neatHack-2026/creative-engine/issues/1",
                "trusted_derivation": "TEST ONLY synthetic input",
                "unavailable_semantics": "TEST ONLY synthetic rule",
                "precision": "TEST ONLY integer/string fixture values",
                "privacy_retention": "TEST ONLY temporary data",
                "compatibility_rule": "TEST ONLY schema-only validation",
            }
            row_hash = contracts.canonical_hash(row)
            row["producer_review"] = make_approval(
                "agent-harness", "Agent and research", row["producer_leaf"],
                "contract-crosswalk:" + row_hash + ":producer_review", row_hash)
            row["consumer_review"] = make_approval(
                "creative-engine", "Creative and fulfillment", row["consumer_leaf"],
                "contract-crosswalk:" + row_hash + ":consumer_review", row_hash)
            field_crosswalk.append(row)

    inventory_raw = _json_bytes({"format": contracts.FIELD_INVENTORY_FORMAT,
                                 "schemas": inventory_schemas})
    inventory_descriptor = artifact("field-inventory.json", inventory_raw)

    default_adapter_source = r'''import base64,hashlib,json,sys
def digest(raw): return hashlib.sha256(raw).hexdigest()
def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False).encode("utf-8")
def valid(schema,data):
    if not isinstance(schema,dict) or schema.get("type")!="object" or not isinstance(data,dict): return False
    required=schema.get("required",[]); props=schema.get("properties",{})
    if not isinstance(required,list) or not isinstance(props,dict) or any(name not in data for name in required): return False
    for name,rule in props.items():
        if name not in data: continue
        if not isinstance(rule,dict): return False
        value=data[name]; kind=rule.get("type")
        matches={"integer":type(value) is int,"string":isinstance(value,str),"boolean":isinstance(value,bool)}.get(kind,False)
        if not matches: return False
        if isinstance(rule.get("enum"),list) and value not in rule["enum"]: return False
    return True
request=json.load(sys.stdin)
results=[]
for item in request["fixtures"]:
    schema_raw=open(item["schema_path"],"rb").read()
    fixture_raw=open(item["fixture_path"],"rb").read()
    schema=json.loads(schema_raw.decode("utf-8")); data=json.loads(fixture_raw.decode("utf-8"))
    output=canonical(data)
    outcome=data.get("decision") if valid(schema,data) else "rejected"
    results.append({"case_id":item["case_id"],"observed_schema_sha256":digest(schema_raw),
        "observed_fixture_sha256":digest(fixture_raw),"contract_id":item["contract_id"],
        "primitive":item["primitive"],"outcome":outcome,
        "serialized_bytes_base64":base64.b64encode(output).decode("ascii")})
print(json.dumps({"protocol":"agent-creative.contract-fixture-adapter/v2",
    "release_revision":request["release_revision"],"results":results},sort_keys=True,separators=(",",":")))
'''.encode("utf-8")
    if adapter_source is None:
        adapter_source = default_adapter_source
    adapter_descriptor = artifact("adapter.py", adapter_source,
                                  protocol=contracts.ADAPTER_PROTOCOL, dependencies=[])
    adapter_approval = make_approval(
        "shared-contracts", "Agent and research", contracts.OWNER_ACTIONS[0]["reference"],
        "contract-validator-adapter", adapter_descriptor["sha256"])
    adapter_descriptor["evidence_id"] = adapter_approval["evidence_id"]

    fixture_rows: list[dict] = []
    fixture_crosswalk: list[dict] = []
    for case_id, expected_outcome in contracts.REQUIRED_CASES.items():
        primitive = next((name for name, required_case in contracts.REQUIRED_PRIMITIVE_FIXTURES.items()
                          if required_case == case_id), "none")
        schema_artifact = schema_by_primitive[primitive]
        contract_id = "SYNTH-CONTRACT-" + primitive
        data = {"decision": expected_outcome,
                "value": "wrong-type" if case_id == "current_invalid" else 7}
        fixture_raw = _json_bytes(data)
        fixture_artifact = artifact("fixtures/" + case_id + ".json", fixture_raw)
        fixture = {
            **fixture_artifact,
            "case_id": case_id,
            "schema_id": schema_artifact["schema_id"],
            "schema_sha256": schema_artifact["sha256"],
            "contract_id": contract_id,
            "primitive": primitive,
            "expected_outcome": expected_outcome,
            "data_classification": "synthetic",
            "media_type": "application/json",
            "serialized_sha256": hashlib.sha256(fixture_raw).hexdigest(),
        }
        fixture_rows.append(fixture)
        fixture_crosswalk.append({
            "case_id": case_id,
            "schema_id": fixture["schema_id"],
            "schema_sha256": fixture["schema_sha256"],
            "contract_id": contract_id,
            "primitive": primitive,
            "fixture_sha256": fixture["sha256"],
        })

    release = {
        "repository": "Jevelopers-neatHack-2026/shared-contracts",
        "revision": "a" * 40,
        "version": "TEST-ONLY-SYNTHETIC",
        "schema_artifacts": schemas,
        "field_inventory": inventory_descriptor,
        "validator_adapter": adapter_descriptor,
        "compatibility": {"unknown_additive_fields": "ignore"},
    }
    release_subject = contracts.canonical_hash({
        "release": release,
        "field_crosswalk": field_crosswalk,
        "fixture_crosswalk": fixture_crosswalk,
        "fixtures": fixture_rows,
    })
    owner_decisions = []
    for requirement in contracts.OWNER_ACTIONS:
        owner = requirement["owner"]
        subject_id = "contract-release:" + owner
        approval = make_approval(owner, "Agent and research", requirement["reference"],
                                 subject_id, release_subject)
        owner_decisions.append({
            "status": "accepted",
            "owner": owner,
            "decision_ref": requirement["reference"],
            "revision": approval["revision"],
            "subject_id": subject_id,
            "subject_sha256": release_subject,
            "evidence_id": approval["evidence_id"],
        })

    manifest = {
        "format": contracts.FORMAT,
        "test_only": True,
        "status": "published",
        "canonical_release": release,
        "field_crosswalk": field_crosswalk,
        "fixtures": fixture_rows,
        "fixture_crosswalk": fixture_crosswalk,
        "owner_decisions": owner_decisions,
    }
    manifest_path = package / "release.json"
    manifest_path.write_bytes(_json_bytes(manifest))
    evidence_index = {
        "format": contracts.OWNER_EVIDENCE_INDEX_FORMAT,
        "classification": "synthetic_test_only",
        "evidence": evidence_rows,
    }
    evidence_path = evidence_root / "index.json"
    evidence_raw = _json_bytes(evidence_index)
    evidence_path.write_bytes(evidence_raw)
    return manifest_path, evidence_path, hashlib.sha256(evidence_raw).hexdigest()
