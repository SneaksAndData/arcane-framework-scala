import requests


def _create_warehouse(name: str, key_prefix: str, namespaces: list[str]) -> None:
    resp = requests.post(
        url="http://localhost:20001/management/v1/warehouse",
        json={
            "warehouse-name": name,
            "project-id": "00000000-0000-0000-0000-000000000000",
            "storage-profile": {
                "type": "s3",
                "bucket": "tmp",
                "key-prefix": key_prefix,
                "assume-role-arn": None,
                "endpoint": "http://localhost:9000",
                "region": "us-east-1",
                "path-style-access": True,
                "flavor": "minio",
                "sts-enabled": True,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": "minioadmin",
                "aws-secret-access-key": "minioadmin",
            },
        },
    )

    if resp.status_code not in [409, 200]:
        resp.raise_for_status()

    warehouse_prefix = requests.get(f"http://localhost:20001/catalog/v1/config?warehouse={name}").json()["defaults"][
        "prefix"
    ]
    for ns in namespaces:
        ns_resp = requests.post(
            f"http://localhost:20001/catalog/v1/{warehouse_prefix}/namespaces", json={"namespace": [ns]}
        )
        if ns_resp.status_code not in [409, 200]:
            ns_resp.raise_for_status()


def init_lk():
    # bootstrap
    resp = requests.post(url="http://localhost:20001/management/v1/bootstrap", json={"accept-terms-of-use": True})
    if resp.status_code not in [200, 400]:
        resp.raise_for_status()

    _create_warehouse("demo", "initial-warehouse", ["test"])


if __name__ == "__main__":
    init_lk()


