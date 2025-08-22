import json
import os

from encompass_to_samsara.samsara_client import SamsaraClient


def main() -> None:
    api_token = os.environ["SAMSARA_BEARER_TOKEN"]
    client = SamsaraClient(api_token=api_token)
    addresses = client.list_addresses(limit=512)
    with open("addresses.json", "w", encoding="utf-8") as f:
        json.dump(addresses, f, indent=2)


if __name__ == "__main__":
    main()
