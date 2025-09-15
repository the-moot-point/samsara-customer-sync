from __future__ import annotations

import json
import logging
import os

import click
from dotenv import load_dotenv

from .samsara_client import SamsaraClient
from .sync_daily import run_daily
from .sync_full import run_full

# Configure root logger
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)sZ %(levelname)s %(name)s :: %(message)s",
)


@click.group(
    help=(
        "Sync Encompass (SoT) → Samsara addresses. Dry-run by default. "
        "Customers with Account Status INACTIVE are skipped unless explicitly deleted."
    )
)
@click.option(
    "--api-rate-config",
    type=click.Path(exists=True, dir_okay=False),
    help=(
        "Path to JSON file with API rate limit settings. "
        "Defaults to built-in client behavior."
    ),
    default=None,
)
@click.pass_context
def cli(ctx: click.Context, api_rate_config: str | None) -> None:
    load_dotenv()  # optional .env
    ctx.ensure_object(dict)
    if api_rate_config:
        with open(api_rate_config, encoding="utf-8") as f:
            ctx.obj["rate_limits"] = json.load(f)
    else:
        ctx.obj["rate_limits"] = None


@cli.command("full", help="Run a full refresh from a complete Encompass CSV.")
@click.option("--encompass-csv", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--warehouses", required=True, type=click.Path(exists=True))
@click.option("--out-dir", required=True, type=click.Path(file_okay=False))
@click.option(
    "--radius-m",
    default=lambda: int(os.getenv("E2S_DEFAULT_RADIUS_METERS", "50")),
    show_default=True,
    type=int,
)
@click.option("--retention-days", default=30, show_default=True, type=int)
@click.option("--confirm-delete", is_flag=True, help="Allow hard deletes after retention window.")
@click.option("--apply", is_flag=True, help="Apply changes. Without this flag, dry-run only.")
@click.option(
    "--progress/--no-progress",
    default=True,
    show_default=True,
    help="Show a progress bar during processing.",
)
@click.pass_context
def full_cmd(
    ctx: click.Context,
    encompass_csv: str,
    warehouses: str,
    out_dir: str,
    radius_m: int,
    retention_days: int,
    confirm_delete: bool,
    apply: bool,
    progress: bool,
) -> None:
    client = SamsaraClient(rate_limits=ctx.obj.get("rate_limits"))
    # Dispatch directly to run_full. All action handling occurs inside run_full
    # which instantiates its own actions list, ensuring this CLI command
    # remains stateless and isolated from other subcommands.
    run_full(
        client,
        encompass_csv=encompass_csv,
        warehouses_path=warehouses,
        out_dir=out_dir,
        radius_m=radius_m,
        apply=apply,
        retention_days=retention_days,
        confirm_delete=confirm_delete,
        progress=progress,
    )


@cli.command("daily", help="Run a daily incremental sync from a delta CSV.")
@click.option("--encompass-delta", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--warehouses", required=True, type=click.Path(exists=True))
@click.option("--out-dir", required=True, type=click.Path(file_okay=False))
@click.option(
    "--radius-m",
    default=lambda: int(os.getenv("E2S_DEFAULT_RADIUS_METERS", "50")),
    show_default=True,
    type=int,
)
@click.option("--retention-days", default=30, show_default=True, type=int)
@click.option("--confirm-delete", is_flag=True, help="Allow hard deletes after retention window.")
@click.option("--apply", is_flag=True, help="Apply changes. Without this flag, dry-run only.")
@click.option(
    "--progress/--no-progress",
    default=True,
    show_default=True,
    help="Show a progress bar during processing.",
)
@click.pass_context
def daily_cmd(
    ctx: click.Context,
    encompass_delta: str,
    warehouses: str,
    out_dir: str,
    radius_m: int,
    retention_days: int,
    confirm_delete: bool,
    apply: bool,
    progress: bool,
) -> None:
    client = SamsaraClient(rate_limits=ctx.obj.get("rate_limits"))
    run_daily(
        client,
        encompass_delta=encompass_delta,
        warehouses_path=warehouses,
        out_dir=out_dir,
        radius_m=radius_m,
        apply=apply,
        retention_days=retention_days,
        confirm_delete=confirm_delete,
        progress=progress,
    )


def main():
    cli()


if __name__ == "__main__":
    main()
