import json

import requests
from confluent_kafka import Message as KafkaMessage

from esgf_core_utils.models.kafka.message_processor import MessageProcessor


class CitationMessageProcessor(MessageProcessor):

    def __init__(self, citation_base_url: str, citation_api_token: str):
        self.citation_base_url = citation_base_url
        self.citation_api_token = citation_api_token

    def post_citation(self, citation_url: str, citation_data):  # stac_item: dict):
        """
        Assemble the citation record for publication.

        Auto-generating a citation record should include:
        - Record Title
        - Primary Default Author or Author from alternative source.
        - Additional Institutions/Funding Streams/Contacts if available
        """

        citation_data["primary"] = json.dumps(
            {"first_name": "Daniel", "last_name": "Westwood"}
        )

        print(citation_data)

        r = requests.post(
            url=f"{self.citation_base_url}/api/citations/",
            data=citation_data,
            headers={"Authorization": f"Token {self.citation_api_token}"},
            timeout=300,
        )
        print(r.status_code)
        print(r.content)

    def citation_exists(self, citation_url: str) -> bool:
        return requests.get(citation_url, timeout=300).status_code == 200

    def ingest(self, message: KafkaMessage):
        """
        Handle a message received from the kafka topic
        """

        # get stac content from message
        stac_publication = message.get("data", {}).get("payload", {}).get("item", None)

        if not stac_publication:
            # Ignore message with no payload content
            return

        # Replace as needed
        required_facets = [
            "cmip7:mip_era",
            "cmip7:activity_id",
            "cmip7:institution_id",
            "cmip7:source_id",
            "cmip7:experiment_id",
        ]

        citation_data = {}

        citation_url = self.citation_base_url + "/api/citation/"
        facet_list = []
        for facet in required_facets:
            facet_list.append(stac_publication["properties"].get(facet))
            citation_data[facet.split(":")[-1]] = stac_publication["properties"].get(
                facet
            )

        citation_url += ".".join(facet_list)

        # citation_url = stac_publication.get('properties').get('citation_url')

        if not self.citation_exists(citation_url):
            self.post_citation(citation_url, citation_data)

        # If citation does exist, update the stac record if the citation_url is not present yet.
