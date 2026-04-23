import json
import os

import click
import yaml

from esgf_core_utils.listeners.citation import CitationMessageProcessor
from esgf_core_utils.models.kafka.consumer import KafkaConsumer

listeners = {"citation": CitationMessageProcessor}


def probe_success(healthcheck: str):

    hdir = "/".join(healthcheck.split("/")[:-1])
    if not os.access(hdir, os.W_OK):
        raise PermissionError("Permission denied accessing healthcheck area")
    open(healthcheck, "a").close()


def probe_fail(healthcheck: str):
    hdir = "/".join(healthcheck.split("/")[:-1])
    if not os.access(hdir, os.W_OK):
        raise PermissionError("Permission denied accessing healthcheck area")
    os.remove(healthcheck)


@click.command()
@click.argument("listener")
@click.argument("config")
@click.argument("secrets")
@click.option("--healthcheck", dest="healthcheck", help="path to healthcheck probe")
def main(listener: str, config: str, secrets: str, healthcheck: str):

    # use importlib to define fail_state, success_state?

    conf = {}
    with open(config) as f:
        conf.update(json.load(f))

    with open(secrets) as f:
        conf.update(yaml.safe_load(f))

    # Start KafkaListener
    if listener not in listeners:
        raise ValueError(
            f'Listener "{listener}" not recognised - available listeners: {list(listeners.keys())}'
        )

    message_processor = listeners.get(listener)(**conf)
    consumer = KafkaConsumer(message_processor=message_processor)
    try:
        if healthcheck:
            probe_success(healthcheck)
        consumer.start()
    except Exception as _:
        if healthcheck:
            probe_fail(healthcheck)


if __name__ == "__main__":
    main()
