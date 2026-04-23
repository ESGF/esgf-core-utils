from esgf_core_utils.models.kafka.consumer import KafkaConsumer
from esgf_core_utils.listeners.citation import CitationMessageProcessor
import click
import json
import yaml
import os

listeners = {"citation": CitationMessageProcessor}


def probe_success():
    if not os.access("/tmp", os.W_OK):
        raise PermissionError("Permission denied accessing healthcheck area")
    os.system("touch /tmp/healthcheck")


def probe_fail():
    if not os.access("/tmp", os.W_OK):
        raise PermissionError("Permission denied accessing healthcheck area")
    os.system("rm -f /tmp/healthcheck")


@click.command()
@click.argument("listener")
@click.argument("config")
@click.argument("secrets")
@click.option("--fail-state", is_flag=True, help="State to transition to on failure")
@click.option("--success-state", is_flag=True, help="State to transition to on success")
def main(
    listener: str, config: str, secrets: str, fail_state: bool, success_state: bool
):

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
        if success_state:
            probe_success()
        consumer.start()
    except:
        if fail_state:
            probe_fail()


if __name__ == "__main__":
    main()
